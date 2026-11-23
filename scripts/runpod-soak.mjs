// scripts/runpod-soak.mjs — half an hour of real activity on the real open markets, with miners arriving.
//
//   RUNPOD_API_KEY=… node scripts/runpod-soak.mjs --minutes 30 --waves 5 \
//     --shard-list 19085,21249,… --mids 2,97,… [--vcpu 8] [--rate 0] [--resting 2000]
//
// What this is for, and why it is not the cells benchmark. `--mode cells` rents C self-contained venues and sums
// them: it answers how fast the cores are and nothing about a venue that grows. This rents ONE broker and then
// adds, every few minutes, a miner pod AND the sequencing slice it covers — because throughput scales with SHARDS
// SPREAD ACROSS MACHINES, not with miner count. Finality needs `threshold` votes, so a shard moves at the speed of
// the threshold-th fastest miner covering it; another replica of a covered shard is verification work and no
// capacity at all. Waves of (miner + its shards) are the only shape that shows the real relationship.
//
// The workload is the live venue's: `scripts/market-snapshot.mjs` gives the open market ids and the odds service's
// fair price for each, and each shard's book is quoted around ITS OWN fair price. A soak with every market at 50c
// produces books the venue never has.
//
// `--threshold` is 1 by default here, and that is a deliberate simplification worth stating: one miner per slice
// finalises its own slice, which isolates throughput-vs-shards from the replica factor. Production runs 2 of 3,
// which multiplies the verification by three — the per-core figure to multiply is in docs/L3-MINERS.md §6.
//
// Every pod is terminated in a finally, on SIGINT, and `node scripts/runpod.mjs sweep` kills strays.
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { createPod, waitRunning, podInfo, endpoint, terminatePod, getJson } from './runpod.mjs';

const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 && process.argv[i + 1] ? process.argv[i + 1] : d; };
const flag = (k) => process.argv.includes(k);

const MINUTES = Number(arg('--minutes', 30));
const WAVES = Number(arg('--waves', 5));
const VCPU = Number(arg('--vcpu', 8));
const BROKER_VCPU = Number(arg('--broker-vcpu', 8));
const RATE = Number(arg('--rate', 0));                 // 0 = each shard finds its own
const RESTING = Number(arg('--resting', 2000));
const BATCH = Number(arg('--batch', 500)), EPOCH = Number(arg('--epoch', 20));
const WORKERS = Number(arg('--workers', 2));
const THRESHOLD = Number(arg('--threshold', 1));
const IMAGE = arg('--image', 'ghcr.io/dubthecat/rolla-miner:latest');
const SIM_IMAGE = arg('--sim-image', 'ghcr.io/dubthecat/rolla-miner-sim:latest');
const BOOK = process.env.PREDICT_BOOK || '0x7197A5160562516F6f8C4503dF03CD836a524D66';
const CHAIN_ID = String(process.env.CHAIN_ID || 46630);
const SHARD_LIST = String(arg('--shard-list', '')).split(',').map((x) => x.trim()).filter(Boolean);
const MIDS = String(arg('--mids', '')).split(',').map((x) => x.trim()).filter(Boolean);
if (!SHARD_LIST.length) { console.error('--shard-list is required (node scripts/market-snapshot.mjs --env)'); process.exit(2); }

const t0 = Date.now();
const say = (...a) => console.log(`+${((Date.now() - t0) / 1000).toFixed(0)}s`, ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const pods = [];
const FLAVORS = ['cpu3c', 'cpu5c', 'cpu3g', 'cpu5g'];
const GPUS = ['NVIDIA GeForce RTX 4090', 'NVIDIA RTX A6000', 'NVIDIA L40S', 'NVIDIA GeForce RTX 3090', 'NVIDIA RTX A5000'];

async function rent(opts) {
  let last = null;
  for (const cloud of ['SECURE', 'COMMUNITY']) {
    for (const flavor of FLAVORS) { try { const p = await createPod({ ...opts, flavor, cloud }); return { ...p, kind: `${cloud} ${flavor}×${opts.vcpu}` }; } catch (e) { last = e; } }
    for (const gpu of GPUS) { try { const p = await createPod({ ...opts, cloud, gpu }); return { ...p, kind: `${cloud} ${gpu}` }; } catch (e) { last = e; } }
  }
  throw new Error(`no capacity for ${opts.name}: ${last && last.message.slice(0, 120)}`);
}
/// `waitRunning` does not always hand back a `url`, and every /results fetch here sits inside a catch — so a
/// missing url made the whole run silent: the waves still ramped, the generators still worked, and the
/// orchestrator reported nothing and computed no scaling. Set it explicitly from the port mapping.
async function up(p, needPorts = []) {
  const info = await waitRunning(p.id, { timeoutMs: 420000, needPorts });
  Object.assign(p, info);
  if (!p.url) p.url = endpoint(p, 8080) || `https://${p.id}-8080.proxy.runpod.net`;
  return p;
}

async function teardown() {
  if (flag('--keep')) { say('--keep: LEAVING', pods.length, 'pod(s) RUNNING AND BILLING:', pods.map((p) => p.id).join(' ')); return; }
  let n = 0;
  for (const p of pods) { try { await terminatePod(p.id); n++; } catch (e) { say('terminate FAILED', p.id, e.message.slice(0, 70)); } }
  say(`terminated ${n}/${pods.length} pod(s) — run \`node scripts/runpod.mjs sweep\` if any failed`);
}
// SIGINT *and* SIGTERM: a plain `kill` is not Ctrl-C, and with only SIGINT handled it killed the orchestrator
// outright and left every rented pod running. Both, and SIGHUP, because the cost of missing one is a bill.
let dying = false;
for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
  process.on(sig, async () => { if (dying) process.exit(130); dying = true; say(`[${sig}] tearing down`); await teardown(); process.exit(130); });
}

/// the broker, advertising the public address RunPod gave it. Redpanda from 26.3 refuses non-loopback clients
/// unless protected_mode is off, and that is a CLUSTER property, so it is seeded in the bootstrap file before the
/// first start — `--set redpanda.X` writes the node config, which does not know the name.
const brokerCmd = ['/bin/sh', '-c',
  'set -e; mkdir -p /etc/redpanda /tmp/rp; ' +
  'printf "protected_mode: false\\nkafka_batch_max_bytes: 8388608\\nauto_create_topics_enabled: true\\ngroup_initial_rebalance_delay: 0\\n" > /etc/redpanda/.bootstrap.yaml; ' +
  'chown -R redpanda:redpanda /etc/redpanda /tmp/rp 2>/dev/null || true; ' +
  'ADV="${RUNPOD_PUBLIC_IP:-127.0.0.1}:${RUNPOD_TCP_PORT_9092:-9092}"; ' +
  'echo "[broker] advertising $ADV"; ' +
  'exec runuser -u redpanda -- /opt/redpanda/bin/rpk redpanda start --overprovisioned --smp 4 --memory 6G ' +
  '--reserve-memory 0M --node-id 0 --check=false --kafka-addr PLAINTEXT://0.0.0.0:9092 ' +
  '--advertise-kafka-addr "PLAINTEXT://$ADV" --rpc-addr 127.0.0.1:33145 --set redpanda.data_directory=/tmp/rp'];

const results = { minutes: MINUTES, waves: WAVES, threshold: THRESHOLD, shards: SHARD_LIST.length, wave: [] };

try {
  const seqKey = generatePrivateKey(), seqAddr = privateKeyToAccount(seqKey).address;
  say(`soak: ${MINUTES} min · ${WAVES} wave(s) of (1 miner pod + its shards) · ${SHARD_LIST.length} real market(s) · threshold ${THRESHOLD} · sequencer ${seqAddr}`);

  const broker = await rent({ name: `soak-broker-${Date.now().toString(36)}`, image: SIM_IMAGE, ports: ['9092/tcp'], vcpu: BROKER_VCPU, diskGb: 80, cmd: brokerCmd, publicIp: true, env: {} });
  pods.push(broker); say('broker', broker.id, broker.kind, `$${broker.costPerHr}/h`);
  await up(broker, ['9092']);
  const BROKERS = endpoint(broker, 9092);
  if (!BROKERS) throw new Error('the broker never got a public 9092 mapping');
  say('broker reachable at', BROKERS);
  // the port opens before redpanda serves; the generator retries its setup, so a short wait is enough
  await sleep(45000);

  // the shards divide between the waves, so each wave's miner covers a slice nobody else does
  const per = Math.max(1, Math.ceil(SHARD_LIST.length / WAVES));
  const everyMs = Math.floor((MINUTES * 60 * 1000) / WAVES);
  const live = [];                                   // { wave, gen, miner, shards }

  for (let w = 1; w <= WAVES; w++) {
    const shards = SHARD_LIST.slice((w - 1) * per, w * per);
    const mids = MIDS.length ? MIDS.slice((w - 1) * per, w * per) : [];
    if (!shards.length) { say(`wave ${w}: no shards left`); break; }
    const row = { wave: w, shards: shards.length, ids: shards.join(',') };
    const wt = Date.now();
    const secs = Math.max(120, Math.floor((MINUTES * 60) - (Date.now() - t0) / 1000));

    // the miner first, so there is something to vote by the time orders arrive
    const keys = shards.map(() => generatePrivateKey());
    const miner = await rent({ name: `soak-miner-${w}-${Date.now().toString(36)}`, image: IMAGE, ports: ['8080/http'], vcpu: VCPU, diskGb: 40, cmd: ['node', 'scripts/sim-miners.mjs'],
      env: { SIM_SHARDS: shards.join(','), SIM_KEYS: keys.join(','), L3_KAFKA_BROKERS: BROKERS, L3_SEQUENCERS: seqAddr,
             L3_THRESHOLD: String(THRESHOLD), SIM_WORKERS: String(WORKERS), PORT: '8080', PREDICT_BOOK: BOOK, CHAIN_ID } });
    pods.push(miner); await up(miner);
    row.minerId = miner.id; row.minerUpMs = Date.now() - wt;
    say(`wave ${w}: miner ${miner.id} (${miner.kind}) up in ${row.minerUpMs}ms for ${shards.length} shard(s)`);

    const gen = await rent({ name: `soak-gen-${w}-${Date.now().toString(36)}`, image: IMAGE, ports: ['8080/http'], vcpu: VCPU, diskGb: 40, cmd: ['node', 'scripts/sim-soak.mjs'],
      env: { SOAK_SECONDS: String(secs), SOAK_SHARD_LIST: shards.join(','), SOAK_MIDS: mids.join(','), SOAK_REPLICAS: '0',
             SOAK_THRESHOLD: String(THRESHOLD), SOAK_SEQ_KEY: seqKey, SOAK_RESTING: String(RESTING), SOAK_RATE: String(RATE),
             SOAK_BATCH: String(BATCH), SOAK_EPOCH: String(EPOCH), L3_KAFKA_BROKERS: BROKERS, L3_LOG_READER: 'native',
             PORT: '8080', PREDICT_BOOK: BOOK, CHAIN_ID } });
    pods.push(gen); await up(gen);
    live.push({ wave: w, gen, miner, shards });
    row.genId = gen.id; row.seconds = secs;
    say(`wave ${w}: generator ${gen.id} up for ${secs}s — the venue now has ${live.reduce((a, x) => a + x.shards.length, 0)} shard(s) on ${live.length} miner pod(s)`);

    const until = Date.now() + (w === WAVES ? Math.max(120000, MINUTES * 60 * 1000 - (Date.now() - t0)) : everyMs);
    const track = [];
    while (Date.now() < until) {
      await sleep(60000);
      const nows = [];
      for (const x of live) { try { const r = await getJson(x.gen.url + '/results'); if (r && r.now) nows.push(r.now); } catch { } }
      if (!nows.length) continue;
      const sum = (k) => nows.reduce((a, y) => a + (Number(y[k]) || 0), 0);
      const p50s = nows.map((y) => y.p50).filter((y) => y != null).sort((a, b) => a - b);
      const t = { elapsed: Math.round((Date.now() - t0) / 1000), miners: live.length, shards: live.reduce((a, x) => a + x.shards.length, 0),
                  rate: sum('rate'), backlog: sum('backlog'), resting: sum('resting'), forks: sum('forks'), dissents: sum('dissents'),
                  halted: sum('halted'), p50: p50s.length ? p50s[Math.floor(p50s.length / 2)] : null };
      track.push(t);
      say(`[${t.elapsed}s] ${t.miners} miner pod(s) · ${t.shards} shard(s) · ${t.rate.toLocaleString()}/s · backlog ${t.backlog} · final p50 ${t.p50}ms · resting ${t.resting.toLocaleString()} · forks ${t.forks} · dissents ${t.dissents} · halted ${t.halted}`);
    }
    row.track = track;
    const last = track.slice(-3);
    row.held = last.length ? Math.round(last.reduce((a, x) => a + x.rate, 0) / last.length) : null;
    row.p50 = last.length ? last[last.length - 1].p50 : null;
    row.forks = last.length ? last[last.length - 1].forks : null;
    results.wave.push(row);
    say(`wave ${w}: held ${(row.held || 0).toLocaleString()}/s with ${live.reduce((a, x) => a + x.shards.length, 0)} shard(s) on ${live.length} miner pod(s)`);
  }

  // the final word from each generator
  for (const x of live) {
    try { const r = await getJson(x.gen.url + '/results'); const s = r.summary || (r.now && { sustainedMedian: r.now.rate }); if (s) say(`wave ${x.wave} summary: held ${(s.sustainedMedian || 0).toLocaleString()}/s · finality p50 ${s.finalityP50 ?? '?'}ms · forks ${s.forks ?? '?'} · caught up ${s.allCaughtUp ?? '?'}`); } catch { }
  }
  const ws = results.wave.filter((w) => w.held);
  if (ws.length > 1) {
    const a = ws[0], b = ws[ws.length - 1];
    say(`SCALING ${(b.held / a.held).toFixed(2)}x from ${a.wave} to ${b.wave} miner pod(s) — ${Math.round(100 * (b.held / a.held) / (b.wave / a.wave))}% of linear`);
    say(`a million orders/s at this per-miner rate: ~${Math.ceil(1e6 / (b.held / b.wave))} miner pod(s) of ${VCPU} vCPU at threshold ${THRESHOLD}`);
  }
  console.log('RESULT ' + JSON.stringify(results));
} catch (e) {
  say('FAILED', e.message);
  console.log('RESULT ' + JSON.stringify({ ...results, error: e.message }));
} finally { await teardown(); }
process.exit(0);
