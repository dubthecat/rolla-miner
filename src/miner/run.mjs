// rolla-l3-miner — one INDEPENDENT L3 miner process. Runs anywhere, holds its own key, trusts nothing.
//
// It consumes a shard's sealed batches from the log, re-verifies every order's EIP-712 signature itself,
// replays the batch into its own book (the JS matcher, or the native book with L3_NATIVE=1), recomputes
// ordersRoot/fillsRoot/bookHash, and publishes a signed vote — or a signed dissent, which halts settlement.
// It keeps its own copy of the log and of everything it signed, so a restart catches up without asking the
// sequencer for anything. docs/L3-MINERS.md is the design; engine/l3/miner/miner.js is the work.
//
//   L3_MINER_KEY=0x… L3_SHARD=4242 L3_LOG=kafka L3_KAFKA_BROKERS=127.0.0.1:9092 \
//   PREDICT_BOOK=0x… CHAIN_ID=46630 DATA_DIR=/data PORT=8090 node engine/l3/miner/run.mjs
//
// env
//   L3_MINER_KEY      required. The miner's key. Never printed, never logged — only its address is.
//   L3_SHARD          ONE shard to mine (the market id, by default): the single-shard miner, exactly as before.
//   L3_SHARDS         or: a comma-separated list of shards, every one mined by this process (fleet.js: one log
//                     connection, one verifier, a miner per shard, one /healthz for all of them).
//   L3_ENGINE_URL     or/and: follow the engine — `GET <url>/v1/l3/markets` every L3_MARKETS_POLL_MS (60000) lists
//                     the books it holds; a miner is started for every shard that appears and stopped for one that
//                     disappears (L3_SHARDS entries are never stopped). L3_SHARD_BY=outcome mirrors an engine that
//                     shards by book. This is how the always-on Fly fleet runs (docs/L3-MINERS.md §10).
//   L3_LOG            file (default) | kafka
//   L3_LOG_DIR        FileLog directory (default <DATA_DIR>/l3/log)
//   L3_KAFKA_BROKERS  host:9092,host2:9092 — with L3_KAFKA_SSL / L3_KAFKA_USER / L3_KAFKA_PASS / L3_KAFKA_MECHANISM
//   DATA_DIR          where the miner's journal lives (default ./miner-data)
//   PREDICT_BOOK      the RollaBook address: with CHAIN_ID it is the EIP-712 domain the orders were signed under.
//                     WRONG VALUE = every order's hash differs = a dissent on every batch, so it is checked at boot.
//                     With the engine on L3_SETTLE=root this is the RollaBookL3 address (orders are signed under it).
//   PREDICT_BOOK_L3   the RollaBookL3 address (alias L3_BOOK_L3). With it, every agreeing vote also carries `finalSig`,
//                     the EIP-712 L3Final the engine brings on chain in RollaBookL3.attest (docs/L3-MINERS.md §4).
//                     Unset: this miner's votes count towards finality off chain but never towards an attestation.
//   CHAIN_ID          default 46630
//   L3_SEQUENCERS     comma-separated addresses allowed to seal batches. Empty: any signature is accepted.
//   L3_THRESHOLD      how many agreeing miners this miner considers final (its own view; default 2)
//   L3_VERIFY_WORKERS signature-verification threads (default min(4, cpus−1); 0 = in this thread)
//   L3_NATIVE=1       replay in native/book/bookd instead of the JS matcher (tick-grid markets only)
//   L3_COMMIT         bookhash (default) | tree — the epoch book commitment (miner/commit-state.js). MUST match the
//                     sequencer's: the two are different words for the same book, so a mismatch is a dissent on
//                     every epoch batch. tree = the incremental Merkle-treap, a root read at the boundary.
//   L3_COMMIT_NATIVE=1 keep the tree in bookd (one --journal none process; L3_NATIVE=1 implies it); L3_COMMIT_FLUSH
//                     buffered updates that force a flush before the batch ends (default 4000)
//   RPC / RPC2        optional: with PREDICT_DESK_V2, session-key grants are checked on chain instead of trusted
//   PORT              /healthz and /metrics (default 8090)
//   L3_LOG_RETRY_MS   fleet mode: how long to wait between attempts to reach the log at boot (default 5000; the
//                     health endpoint answers 503 meanwhile, so a broker that is down shows as such, not as a crash loop)
//   --once            replay what is in the log, print one status line, exit 0 if healthy
import fs from 'node:fs';
import path from 'node:path';
import { createPublicClient, http as viemHttp, getAddress } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { createLog } from './log.js';
import { createMiner } from './miner.js';
import { createFleet, parseShards } from './fleet.js';
import { createVerifier } from './verify.js';
import { BOOK_DOMAIN, DESK_ABI } from '../desk.js';

const env = process.env;
const ONCE = process.argv.includes('--once');
const key = env.L3_MINER_KEY || env.MINER_KEY;
const shard = env.L3_SHARD || env.SHARD;
const fleetShards = parseShards(env.L3_SHARDS);
const engineUrl = (env.L3_ENGINE_URL || '').trim().replace(/\/+$/, '');
const FLEET = !shard && (fleetShards.length > 0 || !!engineUrl);
if (!key) { console.error('L3_MINER_KEY required'); process.exit(1); }
if (!shard && !FLEET) { console.error('L3_SHARD (one shard: the market id, by default), L3_SHARDS (a list) or L3_ENGINE_URL (follow the engine\'s /v1/l3/markets) required'); process.exit(1); }
if (engineUrl && !/^https?:\/\//.test(engineUrl)) { console.error(`L3_ENGINE_URL must be http(s)://…, not ${engineUrl}`); process.exit(1); }
const account = privateKeyToAccount(key.startsWith('0x') ? key : '0x' + key);
const chainId = Number(env.CHAIN_ID || 46630);
const book = env.PREDICT_BOOK || '';
if (!/^0x[0-9a-fA-F]{40}$/.test(book)) { console.error('PREDICT_BOOK required: the RollaBook address is half of the EIP-712 domain the orders were signed under'); process.exit(1); }
const domain = BOOK_DOMAIN(chainId, getAddress(book));
const dataDir = env.DATA_DIR || path.resolve('miner-data');
const dir = path.join(dataDir, 'l3-miner');
fs.mkdirSync(dir, { recursive: true });

const log = (lvl, msg, extra = {}) => process.stdout.write(JSON.stringify({ t: new Date().toISOString(), lvl, msg, miner: account.address, ...(FLEET ? { fleet: true } : { shard: String(shard) }), ...extra }) + '\n');
const say = (...a) => log('info', a.join(' '));

// optional: check session-key grants on chain rather than taking the sequencer's word for them
let grantOf = null;
const desk = env.PREDICT_DESK_V2 || '';
const rpcs = [env.RPC || env.RPC_URL, env.RPC2].filter(Boolean);
if (rpcs.length && /^0x[0-9a-fA-F]{40}$/.test(desk)) {
  const clients = rpcs.map((u) => createPublicClient({ transport: viemHttp(u, { timeout: 15000, retryCount: 1 }) }));
  const cache = new Map();
  grantOf = async (user, signer) => {
    const k = `${user}|${signer}`; const hit = cache.get(k);
    if (hit && Date.now() - hit.at < 30000) return hit.g;
    let last;
    for (const c of clients) {
      try {
        const r = await c.readContract({ address: getAddress(desk), abi: DESK_ABI, functionName: 'grantOf', args: [getAddress(user), getAddress(signer)] });
        const g = { expiry: Number(r.expiry), used: r.used, max: r.maxNotional };
        cache.set(k, { at: Date.now(), g });
        if (cache.size > 5000) for (const kk of [...cache.keys()].slice(0, 1000)) cache.delete(kk);
        return g;
      } catch (e) { last = e; }
    }
    throw last;
  };
  say(`grants checked on chain via ${rpcs.length} RPC(s) against desk ${desk}`);
}

const main = async () => {
  const theLog = await createLog({ env, dir: env.L3_LOG_DIR || path.join(dataDir, 'l3', 'log'), clientId: `rolla-miner-${String(shard)}-${process.pid}`, logger: (m) => log('info', m) });
  const miner = createMiner({
    shard, log: theLog, account, domain, dir, grantOf,
    sequencers: (env.L3_SEQUENCERS || '').split(',').map((s) => s.trim()).filter(Boolean),
    threshold: Number(env.L3_THRESHOLD || 2),
    workers: env.L3_VERIFY_WORKERS != null ? Number(env.L3_VERIFY_WORKERS) : null,
    bookL3: env.PREDICT_BOOK_L3 || env.L3_BOOK_L3 || null,
    env, logger: (m) => log('info', String(m)),
  });
  if (!miner.finalBook) log('warn', 'PREDICT_BOOK_L3 is unset: votes carry no finality signature, so this miner never counts towards an on-chain attestation');
  // an empty allowlist means "any signature": say so, rather than letting it look like a check
  if (!(env.L3_SEQUENCERS || '').trim()) log('warn', 'L3_SEQUENCERS is empty: batches are accepted from any signer');

  await miner.start();
  if (ONCE) {
    // give the log a moment to deliver whatever is already in it, then report
    await new Promise((r) => setTimeout(r, Number(env.L3_ONCE_MS || 3000)));
    const s = miner.status();
    const ok = !s.stalled && s.dissents === 0;
    log(ok ? 'info' : 'error', 'once check', { ok, index: s.index, epoch: s.epoch, batches: s.batches, votes: s.votes, dissents: s.dissents, resting: s.resting, bookHash: s.bookHash, stalled: s.stalled });
    await miner.stop(); await theLog.close();
    process.exit(ok ? 0 : 1);
  }
  miner.serve(Number(env.PORT || 8090));

  let stopping = false;
  const shutdown = async (sig) => {
    if (stopping) return; stopping = true;
    log('info', 'shutting down', { signal: sig, index: miner.index });
    try { await miner.stop(); } catch {}
    try { await theLog.close(); } catch {}
    setTimeout(() => process.exit(0), 500).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('unhandledRejection', (e) => log('error', 'unhandledRejection', { err: e?.message || String(e) }));

  // a periodic line, so a miner's log is readable without scraping /metrics
  const t = setInterval(() => {
    const s = miner.status();
    log(s.stalled ? 'error' : 'info', 'heartbeat', { index: s.index, epoch: s.epoch, batches: s.batches, votes: s.votes, dissents: s.dissents,
      resting: s.resting, lagSeconds: Number(s.lagSeconds.toFixed?.(1) ?? s.lagSeconds), perOrderMs: s.perOrderMs,
      microRolla: s.rewards?.accrued ?? null, finalIndex: s.quorum?.finalIndex ?? null, stalled: s.stalled });
  }, Number(env.L3_HEARTBEAT_MS || 60000));
  t.unref();
};

// ------------------------------------------------------------------------------------------------ the fleet
/// many shards in one process (fleet.js). The health endpoint is up before the log is, and the log is retried
/// until it answers: a miner whose broker is restarting must wait for it, not crash-loop against it.
const mainFleet = async () => {
  const sequencers = (env.L3_SEQUENCERS || '').split(',').map((s) => s.trim()).filter(Boolean);
  const threshold = Number(env.L3_THRESHOLD || 2);
  const bookL3 = env.PREDICT_BOOK_L3 || env.L3_BOOK_L3 || null;
  const workers = env.L3_VERIFY_WORKERS != null ? Number(env.L3_VERIFY_WORKERS) : null;
  const pollMs = Number(env.L3_MARKETS_POLL_MS || 60000);
  const byOutcome = env.L3_SHARD_BY === 'outcome';
  // ONE verifier for every shard: the orders of every market are signed under the same book domain, and a worker
  // pool per shard would be N pools on a machine sized for one
  const verifier = createVerifier({ domain, workers, logger: (m) => log('info', String(m)) });
  let theLog = null;
  const fleet = createFleet({
    static: fleetShards, engineUrl: engineUrl || null, pollMs, byOutcome, address: account.address, logger: (m) => log('info', String(m)),
    newMiner: (s) => createMiner({ shard: s, log: theLog, account, domain, dir, grantOf, verifier, sequencers, threshold, bookL3, env, logger: (m) => log('info', String(m), { shard: String(s) }) }),
  });
  if (!/^0x[0-9a-fA-F]{40}$/.test(bookL3 || '')) log('warn', 'PREDICT_BOOK_L3 is unset: votes carry no finality signature, so this miner never counts towards an on-chain attestation');
  if (!sequencers.length) log('warn', 'L3_SEQUENCERS is empty: batches are accepted from any signer');
  if (!ONCE) fleet.serve(Number(env.PORT || 8090));

  let stopping = false;
  const shutdown = async (sig) => {
    if (stopping) return; stopping = true;
    log('info', 'shutting down', { signal: sig, shards: fleet.shards() });
    try { await fleet.stop(); } catch {}
    try { theLog && (await theLog.close()); } catch {}
    try { await verifier.close(); } catch {}
    setTimeout(() => process.exit(0), 500).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('unhandledRejection', (e) => log('error', 'unhandledRejection', { err: e?.message || String(e) }));

  const retryMs = Number(env.L3_LOG_RETRY_MS || 5000);
  for (let attempt = 1; !theLog && !stopping; attempt++) {
    try { theLog = await createLog({ env, dir: env.L3_LOG_DIR || path.join(dataDir, 'l3', 'log'), clientId: `rolla-miner-fleet-${process.pid}`, logger: (m) => log('info', m) }); }
    catch (e) {
      if (ONCE) throw e;
      log('error', 'log unavailable', { attempt, err: e?.message || String(e), retryMs, brokers: env.L3_KAFKA_BROKERS || null });
      await new Promise((r) => setTimeout(r, retryMs));
    }
  }
  if (stopping) return;
  log('info', 'log connected', { kind: theLog.kind, brokers: theLog.brokers || null, verifyWorkers: verifier.workers, static: fleetShards, engine: engineUrl || null, pollMs, byOutcome, sequencers, threshold, finality: bookL3 || null });
  await fleet.start();
  if (ONCE) {
    await new Promise((r) => setTimeout(r, Number(env.L3_ONCE_MS || 3000)));
    const s = fleet.status();
    log(s.ok ? 'info' : 'error', 'once check', { ok: s.ok, shards: Object.values(s.shards).map((x) => ({ shard: x.shard, index: x.index, batches: x.batches, votes: x.votes, dissents: x.dissents, stalled: x.stalled })) });
    await fleet.stop(); await theLog.close(); await verifier.close();
    process.exit(s.ok ? 0 : 1);
  }
  const t = setInterval(() => {
    const s = fleet.status();
    log(s.ok ? 'info' : 'error', 'heartbeat', { ok: s.ok, shards: s.count, listed: s.engine?.listed?.length ?? null, pollErrors: s.engine?.pollErrors ?? null,
      detail: Object.values(s.shards).map((x) => `${x.shard}:${x.index}/${x.finalIndex ?? '-'}${x.dissents ? ' D' + x.dissents : ''}${x.stalled ? ' STALLED' : ''}`).join(' ') });
  }, Number(env.L3_HEARTBEAT_MS || 60000));
  t.unref();
};

(FLEET ? mainFleet() : main()).catch((e) => { log('error', 'miner failed to start', { err: e?.message || String(e) }); process.exit(1); });
