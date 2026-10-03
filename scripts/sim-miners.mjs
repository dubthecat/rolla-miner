// scripts/sim-miners.mjs — a MINER HOST: one miner process per shard on this machine (the per-process verification
// cap means several processes per machine, L3-MINERS.md §6), with one health endpoint for all of them.
//   SIM_SHARDS=800001,800002 SIM_KEYS=0x…,0x… L3_KAFKA_BROKERS=… L3_SEQUENCERS=0x… L3_THRESHOLD=2 SIM_WORKERS=3 PORT=8080 node scripts/sim-miners.mjs
// GET /healthz → { ok, miners: [{ shard, ok, index, lagSeconds, stalled }] } · GET /metrics → every miner's Prometheus text
import { spawn } from 'node:child_process';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const env = process.env;
const here = path.dirname(fileURLToPath(import.meta.url));
/// one miner process per shard. A miner that exits (a broker not yet serving, a transient error) is restarted
/// with a 3 s backoff, up to `restarts` times; `onLog` gets the exits so a host can show them in its own log.
export function spawnMiners({ shards, keys, brokers, sequencers, threshold, workers, basePort = 9100, dataDir = env.DATA_DIR || '/data', extra = {}, restarts = 8, onLog = (l) => console.log(l) }) {
  const miners = [];
  shards.forEach((shard, i) => {
    const port = basePort + i;
    const m = { shard: String(shard), port, child: null, url: `http://127.0.0.1:${port}`, exits: 0, stopped: false };
    const start = () => {
      m.child = spawn(process.execPath, [path.join(here, '..', 'src', 'miner', 'run.mjs')], {
        stdio: ['ignore', 'inherit', 'inherit'],
        env: { ...env, ...extra, L3_LOG: 'kafka', L3_KAFKA_BROKERS: brokers, L3_SHARD: String(shard), L3_MINER_KEY: keys[i], L3_SEQUENCERS: sequencers, L3_THRESHOLD: String(threshold),
               L3_VERIFY_WORKERS: String(workers), PORT: String(port), DATA_DIR: path.join(dataDir, `m-${shard}`) },
      });
      m.child.on('exit', (code, sig) => {
        m.exits++; onLog(`[sim-miners] miner ${shard} (port ${port}) exited (${code ?? sig}), exit #${m.exits}`);
        if (!m.stopped && m.exits <= restarts) setTimeout(start, 3000); else if (!m.stopped) onLog(`[sim-miners] miner ${shard} gave up after ${m.exits} exits`);
      });
    };
    start(); miners.push(m);
  });
  return miners;
}
const get = async (url, ms = 4000) => { const c = new AbortController(); const t = setTimeout(() => c.abort(), ms); try { const r = await fetch(url, { signal: c.signal }); return await r.text(); } finally { clearTimeout(t); } };
const prom = (txt) => { const o = {}; for (const line of String(txt).split('\n')) { const mm = line.match(/^rolla_l3_([a-z_]+)\{[^}]*\}\s+([-0-9.eE+]+)/); if (mm) o[mm[1]] = Number(mm[2]); } return o; };
/// each miner's health plus where its time went (from its /metrics): ms per order verifying, ms per batch on roots and replay
export async function minersHealth(miners, { timing = true } = {}) {
  const out = [];
  for (const m of miners) {
    try {
      const h = JSON.parse(await get(m.url + '/healthz'));
      const row = { shard: m.shard, ok: !!h.ok, index: h.index, lagSeconds: h.lagSeconds, stalled: h.stalled, dissents: h.dissents, miner: h.miner };
      if (timing) { try { const x = prom(await get(m.url + '/metrics')); const b = x.batches_total || 0; Object.assign(row, { batches: b, orders: x.orders_total, verifyMsPerOrder: x.verify_ms_per_order, rootMsPerBatch: b ? Math.round((x.root_ms_total || 0) / b) : null, applyMsPerBatch: b ? Math.round((x.apply_ms_total || 0) / b) : null, resting: x.resting_orders, votesPending: x.votes_pending, workers: x.verify_workers }); } catch {} }
      out.push(row);
    } catch (e) { out.push({ shard: m.shard, ok: false, error: e.message.slice(0, 60) }); }
  }
  return out;
}
export async function minersMetrics(miners) { const parts = []; for (const m of miners) { try { parts.push(await get(m.url + '/metrics')); } catch {} } return parts.join('\n'); }
export function serveMiners(miners, port) {
  return http.createServer(async (req, res) => {
    try {
      if (req.url === '/metrics') { res.setHeader('content-type', 'text/plain; version=0.0.4'); return res.end(await minersMetrics(miners)); }
      const list = await minersHealth(miners); res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ ok: list.length > 0 && list.every((m) => m.ok), count: list.length, miners: list }));
    } catch (e) { res.statusCode = 500; res.end(JSON.stringify({ ok: false, error: e.message })); }
  }).listen(port, () => console.log(`[sim-miners] serving /healthz and /metrics on ${port}`));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const shards = String(env.SIM_SHARDS || '').split(',').filter(Boolean), keys = String(env.SIM_KEYS || '').split(',').filter(Boolean);
  if (!shards.length || keys.length !== shards.length) { console.error('SIM_SHARDS and SIM_KEYS (one key per shard) are required'); process.exit(2); }
  const miners = spawnMiners({ shards, keys, brokers: env.L3_KAFKA_BROKERS, sequencers: env.L3_SEQUENCERS || '', threshold: Number(env.L3_THRESHOLD || 2), workers: Number(env.SIM_WORKERS || 2) });
  console.log(`[sim-miners] ${miners.length} miner process(es): shards ${shards.join(' ')} · ${env.SIM_WORKERS || 2} verify worker(s) each · broker ${env.L3_KAFKA_BROKERS}`);
  serveMiners(miners, Number(env.PORT || 8080));
  const bye = () => { for (const m of miners) { m.stopped = true; try { m.child && m.child.kill('SIGTERM'); } catch {} } setTimeout(() => process.exit(0), 2000); };
  process.on('SIGTERM', bye); process.on('SIGINT', bye);
}
