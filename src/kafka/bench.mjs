// engine/l3/kafka/bench.mjs — the native reader against kafkajs, on a real broker, doing the job a miner does:
// follow N shards from an offset and receive their records.
//
//   L3_KAFKA_BROKERS=rolla-l3-broker.fly.dev:9092 node engine/l3/kafka/bench.mjs
//   ... --topics 40 --seconds 30 --from tail --only native
//
// `--from tail` follows from near the end of each log, which is the steady state a running fleet is in.
// `--from start` replays each log from its beginning, which is what a restarted miner does and what shows the
// catch-up rate. The default is tail, because that is the state the fleet spends its life in and the one where
// kafkajs's per-shard cost is pure overhead.
//
// What is being compared is not "who parses records faster" — both end up calling JSON.parse on the same bytes.
// It is what it costs to FOLLOW a shard: kafkajs needs a consumer, and a consumer needs a group, and a group
// needs a coordinator.
import { createConnection, metadata, listOffsets, EARLIEST, LATEST } from './client.js';
import { createReader } from './reader.js';

const arg = (f, d = null) => { const i = process.argv.indexOf(f); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const BROKERS = process.env.L3_KAFKA_BROKERS || 'rolla-l3-broker.fly.dev:9092';
const N = Math.max(1, Number(arg('--topics', '20')));
const SECONDS = Math.max(5, Number(arg('--seconds', '30')));
const FROM = arg('--from', 'tail');
const ONLY = arg('--only', null);
const PREFIX = arg('--prefix', 'orders.');
const [HOST, PORT] = BROKERS.split(',')[0].split(':');

const pct = (xs, q) => (xs.length ? [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor(xs.length * q))] : null);
const mb = (n) => (n / 1048576).toFixed(1);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/// topics that exist AND have records, with the offset to start each from
async function pick(conn) {
  const md = await metadata(conn, null);
  const names = md.topics.filter((t) => t.errorCode === 0 && t.name.startsWith(PREFIX)).map((t) => t.name).sort();
  if (!names.length) throw new Error(`no ${PREFIX}* topics on ${BROKERS}`);
  const want = names.slice(0, Math.min(N, names.length));
  const ends = await listOffsets(conn, want.map((t) => ({ topic: t, partition: 0 })), { timestamp: LATEST });
  const starts = await listOffsets(conn, want.map((t) => ({ topic: t, partition: 0 })), { timestamp: EARLIEST });
  const end = new Map(ends.map((e) => [e.topic, e.offset]));
  const begin = new Map(starts.map((e) => [e.topic, e.offset]));
  const out = [];
  for (const t of want) {
    const hi = end.get(t) ?? 0, lo = begin.get(t) ?? 0;
    if (hi <= lo) continue;                                   // empty: nothing to measure
    out.push({ topic: t, from: FROM === 'start' ? lo : Math.max(lo, hi - 200), records: hi - lo });
  }
  return out;
}

async function benchNative(picks) {
  const conn = createConnection({ host: HOST, port: Number(PORT), clientId: 'bench-native' });
  const reader = createReader({ conn, logger: () => { }, partitionMaxBytes: 256 << 10 });
  const first = new Map(); let records = 0;
  const rss0 = process.memoryUsage().rss;
  const t0 = Date.now();
  for (const p of picks) reader.subscribe(p.topic, p.from, async () => { if (!first.has(p.topic)) first.set(p.topic, Date.now() - t0); records++; });
  const subscribeMs = Date.now() - t0;
  await sleep(SECONDS * 1000);
  const s = reader.stats();
  const out = { name: 'native', subscribeMs, firstMs: [...first.values()], covered: first.size, records, rss: process.memoryUsage().rss - rss0,
                detail: `${s.fetches} fetches · ${mb(s.bytes)} MB · ${s.resets} offset reset(s) · ${s.waiting} topic(s) absent · 1 connection, 0 consumer groups` };
  await reader.stop(); conn.close();
  return out;
}

async function benchKafkajs(picks) {
  const { createKafkaLog } = await import('../miner/log.js');
  const log = await createKafkaLog({ brokers: BROKERS, clientId: 'bench-kafkajs', groupPrefix: `bench-${Date.now()}`, logger: () => { } });
  const first = new Map(); let records = 0;
  const rss0 = process.memoryUsage().rss;
  const t0 = Date.now();
  const subs = [];
  // exactly what a fleet member does: one subscription per shard, awaited, one after another
  const done = (async () => { for (const p of picks) subs.push(await log.subscribe(p.topic, p.from, async () => { if (!first.has(p.topic)) first.set(p.topic, Date.now() - t0); records++; })); })();
  await Promise.race([done, sleep(SECONDS * 1000)]);
  const subscribeMs = Date.now() - t0;
  await sleep(Math.max(0, SECONDS * 1000 - subscribeMs));
  const out = { name: 'kafkajs', subscribeMs, firstMs: [...first.values()], covered: first.size, records, rss: process.memoryUsage().rss - rss0,
                detail: `${subs.length} of ${picks.length} subscriptions completed · ${subs.length * 1} consumer group(s), ${subs.length} connection(s)` };
  for (const s of subs) { try { await s.close(); } catch { } }
  try { await log.close(); } catch { }
  return out;
}

(async () => {
  const probe = createConnection({ host: HOST, port: Number(PORT), clientId: 'bench-pick' });
  const picks = await pick(probe);
  probe.close();
  console.log(`broker ${BROKERS} · ${picks.length} topic(s) with records · from ${FROM} · ${SECONDS}s each\n`);
  const rows = [];
  if (ONLY !== 'kafkajs') rows.push(await benchNative(picks));
  if (ONLY !== 'native') rows.push(await benchKafkajs(picks));
  const pad = (s, n) => String(s).padEnd(n);
  console.log(pad('', 9), pad('subscribe', 11), pad('following', 11), pad('first record p50/p90', 22), pad('records', 9), 'rss');
  for (const r of rows) {
    console.log(pad(r.name, 9), pad(r.subscribeMs + ' ms', 11), pad(`${r.covered}/${picks.length}`, 11),
      pad(`${pct(r.firstMs, 0.5) ?? '—'} / ${pct(r.firstMs, 0.9) ?? '—'} ms`, 22), pad(r.records, 9), `${mb(r.rss)} MB`);
    console.log(' '.repeat(9) + r.detail);
  }
  process.exit(0);
})().catch((e) => { console.error('bench failed:', e.message); process.exit(1); });
