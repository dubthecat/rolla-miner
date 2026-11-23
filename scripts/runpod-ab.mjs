// scripts/runpod-ab.mjs — does dividing the shards actually buy throughput? Same machines, same shards, two
// replication factors, back to back.
//
//   RUNPOD_API_KEY=… node scripts/runpod-ab.mjs --minutes 8 --miners 6 --shard-list … --mids … [--replicas 3]
//
// Arm A is what the fleet does today: every miner runs every shard, so the replication factor IS the miner count.
// Arm B holds replication at R and divides the shards between the same miners (src/miner/assign.js). Both arms
// get the same pods, the same shards, the same threshold and the same generators — the ONLY difference is how many
// times each order gets verified. Verification is the whole cost of finality, so if the reasoning is right, arm B
// should sustain roughly (miners / R) times arm A.
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createPod, waitRunning, endpoint, terminatePod, getJson } from './runpod.mjs';
import { assign } from '../src/miner/assign.js';

const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 && process.argv[i + 1] ? process.argv[i + 1] : d; };
const MINUTES = Number(arg('--minutes', 8));
const MINERS = Number(arg('--miners', 6));
const GENS = Number(arg('--gens', 2));
const VCPU = Number(arg('--vcpu', 8));
const R = Number(arg('--replicas', 3));
const THRESHOLD = Number(arg('--threshold', 2));
const RESTING = Number(arg('--resting', 1500));
const IMAGE = arg('--image', 'ghcr.io/dubthecat/rolla-miner:latest');
const SIM_IMAGE = arg('--sim-image', 'ghcr.io/dubthecat/rolla-miner-sim:latest');
const BOOK = process.env.PREDICT_BOOK || '0x7197A5160562516F6f8C4503dF03CD836a524D66';
const CHAIN_ID = String(process.env.CHAIN_ID || 46630);
const SHARDS = String(arg('--shard-list', '')).split(',').map((x) => x.trim()).filter(Boolean);
const MIDS = String(arg('--mids', '')).split(',').map((x) => x.trim()).filter(Boolean);
if (!SHARDS.length) { console.error('--shard-list required'); process.exit(2); }

const t0 = Date.now();
const say = (...a) => console.log(`+${((Date.now() - t0) / 1000).toFixed(0)}s`, ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const pods = [];
const FLAVORS = ['cpu3c', 'cpu5c', 'cpu3g', 'cpu5g'];
const GPUS = ['NVIDIA GeForce RTX 4090', 'NVIDIA RTX A6000', 'NVIDIA L40S', 'NVIDIA GeForce RTX 3090'];
async function rent(opts) {
  let last = null;
  for (const cloud of ['SECURE', 'COMMUNITY']) {
    for (const flavor of FLAVORS) { try { return { ...(await createPod({ ...opts, flavor, cloud })), kind: `${cloud} ${flavor}×${opts.vcpu}` }; } catch (e) { last = e; } }
    for (const gpu of GPUS) { try { return { ...(await createPod({ ...opts, cloud, gpu })), kind: `${cloud} ${gpu}` }; } catch (e) { last = e; } }
  }
  throw new Error(`no capacity for ${opts.name}: ${last && last.message.slice(0, 110)}`);
}
async function up(p, needPorts = []) {
  Object.assign(p, await waitRunning(p.id, { timeoutMs: 420000, needPorts }));
  if (!p.url) p.url = endpoint(p, 8080) || `https://${p.id}-8080.proxy.runpod.net`;
  return p;
}
async function kill(list) { for (const p of list) { try { await terminatePod(p.id); } catch { } const i = pods.indexOf(p); if (i >= 0) pods.splice(i, 1); } }
async function teardown() { let n = 0; for (const p of pods) { try { await terminatePod(p.id); n++; } catch (e) { say('terminate FAILED', p.id); } } say(`terminated ${n}/${pods.length}`); }
// SIGINT *and* SIGTERM: a plain `kill` is not Ctrl-C, and with only SIGINT handled it killed the orchestrator
// outright and left every rented pod running. Both, and SIGHUP, because the cost of missing one is a bill.
let dying = false;
for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
  process.on(sig, async () => { if (dying) process.exit(130); dying = true; say(`[${sig}] tearing down`); await teardown(); process.exit(130); });
}

const brokerCmd = ['/bin/sh', '-c',
  'set -e; mkdir -p /etc/redpanda /tmp/rp; printf "protected_mode: false\\nkafka_batch_max_bytes: 8388608\\nauto_create_topics_enabled: true\\ngroup_initial_rebalance_delay: 0\\n" > /etc/redpanda/.bootstrap.yaml; ' +
  'chown -R redpanda:redpanda /etc/redpanda /tmp/rp 2>/dev/null || true; ADV="${RUNPOD_PUBLIC_IP:-127.0.0.1}:${RUNPOD_TCP_PORT_9092:-9092}"; echo "[broker] $ADV"; ' +
  'exec runuser -u redpanda -- /opt/redpanda/bin/rpk redpanda start --overprovisioned --smp 4 --memory 6G --reserve-memory 0M --node-id 0 --check=false ' +
  '--kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr "PLAINTEXT://$ADV" --rpc-addr 127.0.0.1:33145 --set redpanda.data_directory=/tmp/rp'];

const out = { miners: MINERS, shards: SHARDS.length, replicas: R, threshold: THRESHOLD, arms: [] };

/// one arm: rent miner pods with the given shard assignment, run the generators, measure what it holds
/// Each arm gets its OWN shard ids, and therefore its own topics. Sharing them made arm B resume arm A's log:
/// its miners had to re-verify every batch arm A had written before they could vote on anything new, so the
/// backlog opened at 28,658 and nothing finalised. The fair prices stay with the position, since the shape of the
/// book is what the mid controls — only the id changes.
const armShards = (offset) => SHARDS.map((s) => String(Number(s) + offset));

async function arm(name, perMinerIdx, BROKERS, seqKey, seqAddr, offset) {
  const shards = armShards(offset);
  const perMiner = perMinerIdx.map((idxs) => idxs.map((i) => shards[i]));
  const placements = perMiner.reduce((a, x) => a + x.length, 0);
  say(`--- ${name}: ${MINERS} miner pod(s), ${placements} placement(s) over ${shards.length} shard(s) (ids +${offset}) — each order verified ${(placements / shards.length).toFixed(1)}x ---`);
  const ms = [];
  for (let i = 0; i < MINERS; i++) {
    const sh = perMiner[i];
    const keys = sh.map(() => generatePrivateKey());
    const p = await rent({ name: `ab-${name}-m${i + 1}-${Date.now().toString(36)}`, image: IMAGE, ports: ['8080/http'], vcpu: VCPU, diskGb: 40, cmd: ['node', 'scripts/sim-miners.mjs'],
      env: { SIM_SHARDS: sh.join(','), SIM_KEYS: keys.join(','), L3_KAFKA_BROKERS: BROKERS, L3_SEQUENCERS: seqAddr, L3_THRESHOLD: String(THRESHOLD), SIM_WORKERS: '2', PORT: '8080', PREDICT_BOOK: BOOK, CHAIN_ID } });
    pods.push(p); ms.push(p);
  }
  for (const m of ms) await up(m);
  say(`${name}: ${ms.length} miner pod(s) up`);

  const gens = [];
  const per = Math.ceil(shards.length / GENS);
  for (let g = 0; g < GENS; g++) {
    const sh = shards.slice(g * per, (g + 1) * per), md = MIDS.length ? MIDS.slice(g * per, (g + 1) * per) : [];
    if (!sh.length) break;
    const p = await rent({ name: `ab-${name}-g${g + 1}-${Date.now().toString(36)}`, image: IMAGE, ports: ['8080/http'], vcpu: VCPU, diskGb: 40, cmd: ['node', 'scripts/sim-soak.mjs'],
      env: { SOAK_SECONDS: String(MINUTES * 60), SOAK_SHARD_LIST: sh.join(','), SOAK_MIDS: md.join(','), SOAK_REPLICAS: '0', SOAK_THRESHOLD: String(THRESHOLD),
             SOAK_SEQ_KEY: seqKey, SOAK_RESTING: String(RESTING), SOAK_RATE: '0', SOAK_BATCH: '500', SOAK_EPOCH: '20',
             L3_KAFKA_BROKERS: BROKERS, L3_LOG_READER: 'native', PORT: '8080', PREDICT_BOOK: BOOK, CHAIN_ID } });
    pods.push(p); gens.push(p);
  }
  for (const g of gens) await up(g);
  say(`${name}: ${gens.length} generator(s) up, running ${MINUTES} min`);

  const track = [];
  const until = Date.now() + MINUTES * 60 * 1000;
  while (Date.now() < until) {
    await sleep(60000);
    const nows = [];
    for (const g of gens) { try { const r = await getJson(g.url + '/results'); if (r && r.now) nows.push(r.now); } catch { } }
    if (!nows.length) continue;
    const sum = (k) => nows.reduce((a, x) => a + (Number(x[k]) || 0), 0);
    const p50s = nows.map((x) => x.p50).filter((x) => x != null).sort((a, b) => a - b);
    // `finals` is batches finalised in the sample window, so finals x batch size is the FINALISED order rate —
    // the only honest "sustained" figure. The submit rate says what the generators pushed, and arm A pushed
    // 54,009/s while its backlog went from 55 to 2,919 batches, which is a queue, not throughput.
    const t = { elapsed: Math.round((Date.now() - t0) / 1000), rate: sum('rate'), finalised: sum('finals') * 500,
                backlog: sum('backlog'), resting: sum('resting'),
                forks: sum('forks'), dissents: sum('dissents'), halted: sum('halted'), p50: p50s.length ? p50s[Math.floor(p50s.length / 2)] : null };
    track.push(t);
    say(`${name} [${t.elapsed}s] submit ${t.rate.toLocaleString()}/s · FINALISED ${t.finalised.toLocaleString()}/s · backlog ${t.backlog} · p50 ${t.p50}ms · resting ${t.resting.toLocaleString()} · forks ${t.forks} · dissents ${t.dissents}`);
  }
  // what it HELD: the median of the second half, so the AIMD ramp is not counted as the answer
  const half = track.slice(Math.floor(track.length / 2)).map((x) => x.finalised).sort((a, b) => a - b);
  const submitted = track.slice(Math.floor(track.length / 2)).map((x) => x.rate).sort((a, b) => a - b);
  const row = { name, placements, perOrderVerifications: Number((placements / shards.length).toFixed(2)),
                submit: submitted.length ? submitted[Math.floor(submitted.length / 2)] : null,
                held: half.length ? half[Math.floor(half.length / 2)] : null,
                p50: track.length ? track[track.length - 1].p50 : null,
                backlog: track.length ? track[track.length - 1].backlog : null,
                forks: track.length ? track[track.length - 1].forks : null, track };
  out.arms.push(row);
  say(`${name}: FINALISED ${(row.held || 0).toLocaleString()}/s (submitted ${(row.submit || 0).toLocaleString()}/s) · p50 ${row.p50}ms · backlog ${row.backlog} · forks ${row.forks}`);
  await kill([...ms, ...gens]);
  return row;
}

try {
  const seqKey = generatePrivateKey(), seqAddr = privateKeyToAccount(seqKey).address;
  const broker = await rent({ name: `ab-broker-${Date.now().toString(36)}`, image: SIM_IMAGE, ports: ['9092/tcp'], vcpu: 8, diskGb: 80, cmd: brokerCmd, publicIp: true, env: {} });
  pods.push(broker); await up(broker, ['9092']);
  const BROKERS = endpoint(broker, 9092);
  if (!BROKERS) throw new Error('no public 9092 mapping');
  say('broker', BROKERS);
  await sleep(45000);

  const idx = SHARDS.map((_, i) => i);
  // arm A: every miner runs every shard, which is replication = miner count
  await arm('A-all-shards', Array.from({ length: MINERS }, () => idx.slice()), BROKERS, seqKey, seqAddr, 0);

  // arm B: the same miners, replication held at R, shards divided by rendezvous hashing — on fresh ids
  const addrs = Array.from({ length: MINERS }, (_, i) => '0x' + String(i + 1).padStart(2, '0').repeat(20));
  const a = assign({ shards: idx.map(String), miners: addrs, replicas: R });
  const divided = addrs.map((m) => (a.byMiner.get(m.toLowerCase()) || []).map(Number));
  say(`assignment: ${divided.map((x) => x.length).join('/')} shard(s) per miner (cap ${a.cap}, ideal ${a.ideal.toFixed(1)})`);
  await arm(`B-divided-R${R}`, divided, BROKERS, seqKey, seqAddr, 400000);

  const [A, B] = out.arms;
  if (A && B && A.held && B.held) {
    say(`RESULT dividing the shards: ${(B.held / A.held).toFixed(2)}x the FINALISED rate (${A.held.toLocaleString()}/s → ${B.held.toLocaleString()}/s), backlog ${A.backlog} → ${B.backlog}`);
    say(`verification per order went ${A.perOrderVerifications}x → ${B.perOrderVerifications}x, so the ceiling should move ~${(A.perOrderVerifications / B.perOrderVerifications).toFixed(2)}x`);
  }
  console.log('RESULT ' + JSON.stringify(out));
} catch (e) { say('FAILED', e.message); console.log('RESULT ' + JSON.stringify({ ...out, error: e.message })); }
finally { await teardown(); }
process.exit(0);
