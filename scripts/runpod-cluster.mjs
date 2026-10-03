// scripts/runpod-cluster.mjs — a miner cluster on RunPod: one Redpanda broker pod + N miner pods from the public
// image, a sequencer running HERE feeding signed orders over the public Kafka port, an observer quorum reading the
// votes, every miner's /metrics, then teardown. Measures what the in-process test cannot: real machines, real
// network, finality latency across them.
//   RUNPOD_API_KEY=… node scripts/runpod-cluster.mjs [--miners 3] [--orders 4000] [--vcpu 2] [--flavor cpu3c] [--chaos] [--keep]
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createPod, waitRunning, endpoint, terminatePod, getJson, podInfo } from './runpod.mjs';
import { createKafkaLog, ordersTopic, votesTopic } from '../src/miner/log.js';
import { createSequencer } from '../src/miner/sequencer.js';
import { createShardState } from '../src/miner/miner.js';
import { createQuorum } from '../src/miner/quorum.js';
import { orderHash } from '../src/miner/verify.js';
import { BOOK_DOMAIN, L3_ORDER_TYPES, l3OrderFor, serializeL3Order } from '../src/desk.js';

const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : d; };
const flag = (k) => process.argv.includes(k);
const N_MINERS = Number(arg('--miners', 3)), N_ORDERS = Number(arg('--orders', 4000)), VCPU = Number(arg('--vcpu', 2)), FLAVOR = arg('--flavor', 'cpu3c');
const IMAGE = arg('--image', 'ghcr.io/dubthecat/rolla-miner:latest');
const SHARD = arg('--shard', String(900000 + Math.floor(Math.random() * 99999))), MARKET = Number(SHARD), OUTCOME = 0;
const BOOK = process.env.PREDICT_BOOK || '0x7197A5160562516F6f8C4503dF03CD836a524D66', CHAIN_ID = Number(process.env.CHAIN_ID || 46630);
const THRESHOLD = Math.max(1, N_MINERS - 1);   // 2-of-3: one miner may be slow, dead or lying
const t0 = Date.now(); const ts = () => `+${((Date.now() - t0) / 1000).toFixed(0)}s`;
const say = (...a) => console.log(ts(), ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const pods = [];
async function teardown(why) {
  if (flag('--keep')) { say('keeping pods (--keep):', pods.map((p) => p.id).join(' ')); return; }
  for (const p of pods) { try { await terminatePod(p.id); say('terminated', p.name, p.id); } catch (e) { say('terminate failed', p.id, e.message); } }
  if (why) say('teardown:', why);
}
process.on('SIGINT', async () => { await teardown('interrupted'); process.exit(130); });

try {
  // ---- 1. the broker: Redpanda advertising the public ip:port RunPod hands it ----
  say(`cluster: ${N_MINERS} miners × ${VCPU} vCPU (${FLAVOR}) · ${N_ORDERS} orders · shard ${SHARD} · threshold ${THRESHOLD}`);
  // the broker: by default the Fly-hosted Redpanda (infra/broker; a dedicated IPv4 on 9092 — RunPod's public TCP mappings
  // on CPU pods never answered from outside in testing, while its HTTP proxy did), or a broker pod on RunPod with --broker-pod
  let brokers = arg('--brokers', process.env.L3_KAFKA_BROKERS || 'rolla-l3-broker.fly.dev:9092');
  if (flag('--broker-pod')) {
    const broker = await createPod({ name: `broker-${SHARD}`, image: 'redpandadata/redpanda:latest', ports: ['9092/tcp'], vcpu: 2, flavor: FLAVOR, diskGb: 10, entrypoint: ['sh', '-c'],
      cmd: ['exec redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://$RUNPOD_PUBLIC_IP:$RUNPOD_TCP_PORT_9092'] });
    pods.push(broker); say('broker pod', broker.id, `$${broker.costPerHr}/h`);
    const bp = await waitRunning(broker.id, { needPorts: [9092], timeoutMs: 420000 });
    brokers = endpoint(bp, 9092); say('broker running at', brokers, 'public ip', bp.publicIp);
  } else say('broker', brokers);
  let log = null;
  for (let i = 0; i < 24 && !log; i++) { try { log = await createKafkaLog({ brokers: [brokers], clientId: 'runpod-cluster', logger: () => {} }); } catch (e) { if (i % 4 === 0) say('kafka not ready yet:', e.message.slice(0, 80)); await sleep(5000); } }
  if (!log) throw new Error('could not reach the broker over its public port');
  say('kafka reachable');

  // ---- 2. the miners ----
  const seqAcct = privateKeyToAccount(generatePrivateKey());
  const minerKeys = Array.from({ length: N_MINERS }, () => generatePrivateKey());
  const minerAddrs = minerKeys.map((k) => privateKeyToAccount(k).address);
  const miners = [];
  for (let i = 0; i < N_MINERS; i++) {
    const p = await createPod({ name: `miner-${SHARD}-${i + 1}`, image: IMAGE, ports: ['8080/http'], vcpu: VCPU, flavor: FLAVOR, diskGb: 10,
      env: { L3_LOG: 'kafka', L3_KAFKA_BROKERS: brokers, L3_SHARD: SHARD, L3_MINER_KEY: minerKeys[i], PREDICT_BOOK: BOOK, CHAIN_ID: String(CHAIN_ID), L3_SEQUENCERS: seqAcct.address, L3_THRESHOLD: String(THRESHOLD), L3_VERIFY_WORKERS: String(Math.max(1, VCPU - 1)), PORT: '8080', DATA_DIR: '/data' } });
    pods.push(p); miners.push({ ...p, address: minerAddrs[i] }); say('miner pod', i + 1, p.id, minerAddrs[i], `$${p.costPerHr}/h`);
  }
  for (const m of miners) { const info = await waitRunning(m.id, { timeoutMs: 420000 }); m.url = endpoint(info, 8080); }
  for (const m of miners) { let ok = false; for (let i = 0; i < 60 && !ok; i++) { try { const h = await getJson(m.url + '/healthz'); ok = h && (h.ok === true || h.status === 'ok' || h.index != null); if (!ok && i % 6 === 0) say(m.name, 'health:', JSON.stringify(h).slice(0, 100)); } catch {} if (!ok) await sleep(5000); } say(m.name, ok ? 'healthy' : 'NOT healthy after 5 min', m.url); }

  // ---- 3. the sequencer here, an observer quorum on the votes ----
  const domain = BOOK_DOMAIN(CHAIN_ID, BOOK);
  const sealedAt = new Map(), finalAt = new Map(); const sealed = [], finals = [], forks = [];
  const state = createShardState({});
  const seq = createSequencer({ shard: SHARD, log, account: seqAcct, state, batchMs: 60, batchMax: 250, epochBatches: 10, onSealed: (b) => { sealed.push(b); sealedAt.set(b.index, Date.now()); }, logger: () => {} });
  await seq.resume();
  const quorum = createQuorum({ threshold: THRESHOLD, onFinal: (f) => { finals.push(f); finalAt.set(f.index, Date.now()); }, onFork: (f) => forks.push(f), logger: () => {} });
  await log.subscribe(ordersTopic(SHARD), 0, async ({ value }) => { quorum.announce(value); });
  await log.subscribe(votesTopic(SHARD), 0, async ({ value }) => { await quorum.vote(value); });

  // ---- 4. the workload: matcher.test.mjs's shape, signed by eight traders ----
  const E = 10n ** 18n; const token = '0x' + 'c0'.repeat(20);
  const traders = Array.from({ length: 8 }, () => privateKeyToAccount(generatePrivateKey()));
  async function workload(n, seed, offset) {
    let s = seed >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296; const live = [], plan = [];
    for (let i = 0; i < n; i++) {
      if (live.length && rnd() < 0.1) { plan.push({ t: 'cancel', target: live.splice(Math.floor(rnd() * live.length), 1)[0] }); continue; }
      const buy = rnd() < 0.5, cross = rnd() < 0.3, mid = 50, off = Math.floor(rnd() * 20);
      const cents = buy ? (cross ? mid + off : mid - 1 - off) : (cross ? mid - off : mid + 1 + off);
      const acct = traders[Math.floor(rnd() * traders.length)];
      const order = l3OrderFor({ user: acct.address, marketId: MARKET, outcome: OUTCOME, token, buy, price: BigInt(cents) * E / 100n, size: BigInt(1 + Math.floor(rnd() * 100)) * E, ioc: rnd() < 0.1 });
      order.nonce = BigInt(1700000000000 + offset + i); order.salt = BigInt(offset + i) * 7919n + 1n;
      plan.push({ t: 'add', acct, order }); if (!order.ioc) live.push(plan.length - 1);
    }
    for (const p of plan) if (p.t === 'add') { const order = serializeL3Order(p.order); const sig = await p.acct.signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: p.order }); p.op = { t: 'add', hash: orderHash(domain, order), market: MARKET, outcome: OUTCOME, order, sig, signer: p.acct.address.toLowerCase(), at: 1700000000000 }; }
    for (const p of plan) if (p.t === 'cancel') p.op = { t: 'cancel', hash: plan[p.target].op.hash, market: MARKET, outcome: OUTCOME };
    return plan.map((p) => p.op);
  }
  const run = async (label, ops) => {
    const a = Date.now(); const first = seq.index;
    for (let i = 0; i < ops.length; i++) { seq.submit(ops[i]); if (i % 25 === 0) await new Promise((r) => setImmediate(r)); }
    await seq.flush(); const seqMs = Date.now() - a; const last = seq.index - 1;
    const deadline = Date.now() + 300000; while (Date.now() < deadline && quorum.finalIndex < last) await sleep(50);
    const clusterMs = Date.now() - a;
    const lat = sealed.filter((b) => b.index >= first && b.index <= last).map((b) => (finalAt.get(b.index) || 0) - sealedAt.get(b.index)).filter((x) => x > 0).sort((x, y) => x - y);
    const p = (q) => lat.length ? lat[Math.min(lat.length - 1, Math.floor(q * lat.length))] : null;
    say(`${label}: ${ops.length} ops · batches ${first}..${last} · sequenced in ${seqMs} ms (${Math.round(ops.length / seqMs * 1000)} orders/s) · final at ${quorum.finalIndex} after ${clusterMs} ms (${Math.round(ops.length / clusterMs * 1000)} orders/s cluster) · finality latency seal→quorum p50 ${p(0.5)} ms p90 ${p(0.9)} ms max ${lat[lat.length - 1] ?? null} ms · forks ${forks.length} · halted ${quorum.halted}`);
    return { first, last, final: quorum.finalIndex };
  };
  say('signing the workload…'); const ops = await workload(N_ORDERS, 11, 0); say('signed', ops.length, 'ops');
  const r1 = await run('main run', ops);
  const metrics = async () => { for (const m of miners) { try { const x = await getJson(m.url + '/metrics'); say(m.name, JSON.stringify({ index: x.index, epoch: x.epoch, votes: x.votes, dissents: x.dissents, badSigs: x.badSigs, verifyMs: x.verifyMs ?? x.verify?.ms, matchMs: x.matchMs ?? x.match?.ms, rewards: x.rewards ?? x.microRolla, stalled: x.stalled }).slice(0, 300)); } catch (e) { say(m.name, 'metrics failed', e.message.slice(0, 60)); } } };
  await metrics();

  // ---- 5. chaos: one miner dies, the other two keep finalizing ----
  if (flag('--chaos') && N_MINERS >= 3) {
    const victim = miners[miners.length - 1]; say('chaos: terminating', victim.name); await terminatePod(victim.id); pods.splice(pods.findIndex((p) => p.id === victim.id), 1);
    const ops2 = await workload(Math.min(1000, N_ORDERS), 23, N_ORDERS + 10);
    await run('with one miner down', ops2); await metrics();
  }
  const books = new Set(); for (const m of miners) { try { const x = await getJson(m.url + '/metrics'); if (x.bookHash) books.add(x.bookHash); } catch {} }
  say('sequencer bookHash', state.bookHash(), '· distinct miner bookHashes seen:', books.size ? [...books].join(' ') : '(not exposed)');
  await seq.stop(); try { await log.close?.(); } catch {}
  say(`RESULT finalized ${quorum.finalIndex + 1} batches of ${sealed.length} sealed · ${finals.length} finals · ${forks.length} forks`);
} catch (e) { say('FAILED:', e.message); process.exitCode = 1; }
finally { await teardown(); }
