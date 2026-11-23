// engine/l3/miner/fleet.js — one miner PROCESS that mines many shards: the always-on fleet member (docs/L3-MINERS.md §10).
//
// A miner (miner.js) is one shard: one key, one book, one pair of topics. A venue has a shard per L3 market, and
// the markets come and go, so a machine that must mine "whatever the engine sequences" needs a shard list that
// moves. This is that: a set of miners keyed by shard, reconciled against
//
//   - a static list   (`static`, from L3_SHARDS — a shard named here is mined for as long as the process runs), and
//   - the engine's own list (`engineUrl`: `GET <url>/v1/l3/markets` every `pollMs`, the books the engine holds; a
//     shard is the market id, or `market-outcome` when the engine shards by outcome — the same rule as
//     sequencer.js's `shardOf`, mirrored here with `byOutcome`).
//
// A shard that appears gets a miner (started from its own journal, so a restart or a re-listing resumes where it
// left off and the first batches are replayed from the log's beginning); a shard the engine stops listing gets its
// miner stopped, journal kept. A poll that fails changes nothing — the last answer stands — because a flapping
// engine must not take the miners off a shard that is still being sequenced.
//
// Every miner shares the process's one log connection and ONE signature verifier (the orders of every shard are
// signed under the same book domain), so N shards cost N books and 2N log subscriptions, not N worker pools.
//
// The fleet also answers /healthz and /metrics for all of its miners at once, in the shape miner.js's own server
// uses, with a `shard` label per series. Health is strict about what matters and lenient about what does not: a
// stalled miner, a dissent, or votes the log refuses is 503; an idle shard (no batch for an hour) is fine — the
// engine simply has nothing to sequence there.
import http from 'node:http';
import { shardsFor } from './assign.js';
import fs from 'node:fs';
import path from 'node:path';

/// `L3_SHARDS=13805,13839` → ['13805', '13839'] (deduplicated, trimmed, in order)
export const parseShards = (s) => [...new Set(String(s || '').split(',').map((x) => x.trim()).filter(Boolean))];

/// the shards behind the engine's /v1/l3/markets answer: [{ market, outcome, ... }] → unique shard ids, in the
/// sequencer's own naming (a market, or `market-outcome` with byOutcome), sorted numerically
export function shardsOfMarkets(markets, { byOutcome = false } = {}) {
  const out = new Set();
  for (const m of Array.isArray(markets) ? markets : []) {
    if (m == null) continue;
    const market = Number(typeof m === 'object' ? (m.market ?? m.id) : m);
    if (!Number.isFinite(market) || market < 0) continue;
    const outcome = Number(typeof m === 'object' ? (m.outcome ?? 0) : 0);
    out.add(byOutcome ? `${market}-${Number.isFinite(outcome) ? outcome : 0}` : String(market));
  }
  return [...out].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
}

/**
 * createFleet({ newMiner, static, engineUrl, pollMs, byOutcome, fetchImpl, logger, address })
 *   newMiner(shard)  → a miner (createMiner's api: start/stop/status/metrics/quorum/stalled) — the fleet owns its life
 *   static           shards mined unconditionally (L3_SHARDS)
 *   engineUrl        follow `<engineUrl>/v1/l3/markets` (L3_ENGINE_URL); null = the static list only
 *   pollMs           how often the engine is asked (L3_MARKETS_POLL_MS, default 60 s)
 *   byOutcome        the engine runs L3_SHARD_BY=outcome (a shard per book)
 *   fetchImpl        fetch (tests inject one)
 *   address          the miner's address, for /healthz and the metric labels
 */
export function createFleet({ newMiner, static: staticShards = [], engineUrl = null, pollMs = 60000, byOutcome = false, fetchImpl = globalThis.fetch, logger: logFn = console.log, address = '', pollTimeoutMs = 15000, journalDir = null, journalRetainDays = 7, journalSweepMs = 6 * 3600000, now = Date.now, env = process.env } = {}) {
  if (typeof newMiner !== 'function') throw new Error('a fleet needs newMiner(shard)');
  const url = engineUrl ? String(engineUrl).replace(/\/+$/, '') : null;
  const fixed = parseShards(staticShards.join ? staticShards.join(',') : staticShards);
  const miners = new Map();       // shard → { miner, since, source: 'static' | 'engine' }
  const stats = { polls: 0, pollErrors: 0, lastPoll: 0, lastPollOk: 0, lastPollError: null, listed: [], started: 0, stopped: 0, startFailures: 0, reconciles: 0 };
  // The journal sweep. A miner writes `<shard>.batches.jsonl` and `<shard>.votes.jsonl` for every shard it mines and
  // nothing ever removed them: 458 files holding 958 MB on a 1 GB volume by 2026-11-11, oldest from 2026-10-04, and
  // all three machines of the fleet at 100 % — so every journal append failed with ENOSPC and no miner could record
  // what it had signed. A miner that restarts with no journal re-derives from the log instead of replaying, which is
  // the slow path at best and impossible once the log's own 7-day retention has passed, and it is consistent with the
  // 44 forks the engine was reporting.
  //
  // What makes a journal safe to delete is that nothing can ask for it again. It is read in exactly one place —
  // replayJournal(), when a miner for THAT shard starts — so a shard the fleet does not mine has no reader. 394 of
  // those 458 files, 798 MB of the 958 MB, belonged to shards whose markets had left the engine's snapshot entirely.
  // The age test is the second half: a shard dropped and re-listed within the window keeps its journal and resumes by
  // replay rather than re-reading the log.
  const jstats = { sweeps: 0, removed: 0, freedBytes: 0, kept: 0, lastAt: null, lastRemoved: 0, error: null };
  function sweepJournals() {
    if (!journalDir) return jstats;
    // Judge by INTENT, not by what has started. `miners` fills in one shard at a time and a shard takes the better
    // part of a minute to come up (its journal is replayed op by op), so on a fleet of 163 shards the first reconcile
    // runs for hours — gating the sweep behind it, which is what I did first, means it effectively never runs. The
    // engine's listing plus the static list is the set this fleet means to mine, and it is known as soon as the first
    // poll returns. Until that poll succeeds the sweep does nothing: a half-known target would delete the journal of
    // a shard that is about to be mined, which is the one mistake here that costs a log re-read.
    if (url && !stats.lastPollOk) return jstats;
    const keep = new Set([...fixed.map(String), ...(stats.listed || []).map(String), ...[...miners.keys()].map(String)]);
    const t0 = now(); const cutoff = t0 - Math.max(0, journalRetainDays) * 86400000;
    let removed = 0, freed = 0, kept = 0;
    try {
      for (const f of fs.readdirSync(journalDir)) {
        const m = /^(.+?)\.(batches|votes)\.jsonl(\..*)?$/.exec(f); if (!m) continue;
        const shard = m[1];
        // A `.corrupt.` copy is a journal a miner set aside after a failed replay. Nothing ever reads one again, so
        // it is judged on age alone — the mined/not-mined test does not protect it, and the hot shards each kept one
        // of 78 MB on a 3 GB volume.
        const forensic = /\.corrupt\./.test(f);
        if (!forensic && keep.has(shard)) { kept++; continue; }     // mined, or about to be: its journal is live
        const full = path.join(journalDir, f);
        let st; try { st = fs.statSync(full); } catch { continue; }
        if (st.mtimeMs > cutoff) { kept++; continue; }              // dropped only recently: it may be re-listed
        try { fs.unlinkSync(full); removed++; freed += st.size; } catch { }
      }
    } catch (e) { jstats.error = e.message; logFn(`[l3fleet] journal sweep failed: ${e.message}`); return jstats; }
    jstats.sweeps++; jstats.removed += removed; jstats.freedBytes += freed; jstats.kept = kept;
    jstats.lastAt = new Date(t0).toISOString(); jstats.lastRemoved = removed;
    if (removed) logFn(`[l3fleet] journal sweep: ${removed} file(s) of unmined shards older than ${journalRetainDays} d removed, ${Math.round(freed / 1e6)} MB freed, ${kept} kept`);
    return jstats;
  }
  let timer = null, jtimer = null, stopping = false, started = false, chain = Promise.resolve(), ready = false;
  // Dividing the shards between the fleet's miners instead of giving every shard to every miner. Off by default:
  // turning it on changes nothing until the roster is bigger than the replication factor (with three miners and
  // R=3 the assignment is "everyone runs everything", which is exactly today), and it is the engine that publishes
  // the roster, so a fleet that cannot see one keeps the old behaviour.
  const ASSIGN = String(env.L3_SHARD_ASSIGN ?? '0') === '1';
  const REPLICAS = Math.max(1, Number(env.L3_SHARD_REPLICAS || 3));
  const astats = { on: ASSIGN, replicas: REPLICAS, roster: 0, everyone: null, mine: 0, held: [], under: 0, lastAt: 0 };

  /// the shards wanted right now: the static list plus whatever the engine lists (or last listed, on a failed poll)
  async function wanted() {
    const set = new Set(fixed);
    if (url) {
      stats.polls++;
      try {
        const r = await fetchImpl(`${url}/v1/l3/markets`, { signal: AbortSignal.timeout(pollTimeoutMs), headers: { accept: 'application/json' } });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = await r.json();
        stats.listed = shardsOfMarkets(d?.markets, { byOutcome });
        stats.roster = Array.isArray(d?.miners) ? d.miners.map((a) => String(a).toLowerCase()).filter(Boolean) : [];
        if (Number(d?.replicas) > 0) astats.replicas = Math.max(1, Number(d.replicas));
        stats.lastPollOk = Date.now(); stats.lastPollError = null;
      } catch (e) {
        stats.pollErrors++; stats.lastPollError = e?.message || String(e);
        logFn(`[l3fleet] ${url}/v1/l3/markets: ${stats.lastPollError} — keeping the last answer (${stats.listed.length} shard(s))`);
      }
      stats.lastPoll = Date.now();
      for (const s of stats.listed) set.add(s);
    }
    if (!ASSIGN) return set;
    // Our slice of them. `fixed` (L3_SHARDS) is never divided: a shard named explicitly is one this operator asked
    // for, and the assignment has no business taking it away.
    const roster = stats.roster || [];
    if (!roster.length || !roster.includes(String(address).toLowerCase())) {
      astats.roster = roster.length; astats.everyone = true; astats.mine = set.size; astats.held = []; astats.lastAt = Date.now();
      return set;                                   // not on the roster, or no roster published: behave as before
    }
    const divisible = [...set].filter((x) => !fixed.includes(String(x)));
    const r = shardsFor(address, { shards: divisible, miners: roster, replicas: astats.replicas, running: [...miners.keys()].filter((x) => !fixed.includes(String(x))) });
    const out = new Set([...fixed, ...r.mine]);
    Object.assign(astats, { roster: roster.length, everyone: r.everyone, mine: out.size, held: r.held, under: r.under.length, lastAt: Date.now() });
    if (r.added.length || r.dropped.length || r.held.length) {
      logFn(`[l3fleet] assignment: ${out.size} of ${set.size} shard(s) are mine across ${roster.length} miner(s) at R=${astats.replicas}` +
        `${r.added.length ? ` · +${r.added.length}` : ''}${r.dropped.length ? ` · -${r.dropped.length}` : ''}` +
        `${r.held.length ? ` · holding ${r.held.length} nobody else covers yet` : ''}${r.under.length ? ` · ${r.under.length} under-replicated` : ''}`);
    }
    return out;
  }

  async function startShard(shard, source) {
    let miner;
    try {
      miner = newMiner(shard);
      await miner.start();
      miners.set(shard, { miner, since: Date.now(), source });
      stats.started++;
      logFn(`[l3fleet] mining ${shard} (${source}) · ${miners.size} shard(s)`);
    } catch (e) {
      stats.startFailures++;
      logFn(`[l3fleet] ${shard}: could not start (${e?.message || e}); retrying on the next poll`);
      try { miner && (await miner.stop()); } catch {}
    }
  }
  async function stopShard(shard, why) {
    const e = miners.get(shard); if (!e) return;
    miners.delete(shard);
    try { await e.miner.stop(); } catch (err) { logFn(`[l3fleet] ${shard}: stop failed (${err?.message || err})`); }
    stats.stopped++;
    logFn(`[l3fleet] stopped ${shard} (${why}) · ${miners.size} shard(s)`);
  }

  /// one pass: ask, then start what is missing and stop what is gone. Serialised: a slow poll never overlaps the next.
  function reconcile() {
    chain = chain.then(async () => {
      if (stopping) return;
      stats.reconciles++;
      const set = await wanted();
      if (stopping) return;
      // Sweep BEFORE the first shard starts, not after the reconcile finishes. The post-reconcile sweep below is
      // hours late on a large fleet — a shard takes the better part of a minute to replay its journal — and a miner
      // whose volume is already full cannot append what it signs, which is how the last outage went: ENOSPC, then
      // truncated journals, then 2,811 forks. The first successful poll is the earliest moment the keep set is
      // known, and that is the moment the space is needed.
      if (journalDir && !jstats.sweeps) sweepJournals();
      for (const s of set) if (!miners.has(s)) await startShard(s, fixed.includes(s) ? 'static' : 'engine');
      for (const s of [...miners.keys()]) if (!set.has(s)) await stopShard(s, 'no longer listed by the engine');
      ready = true;
    }).catch((e) => logFn(`[l3fleet] reconcile failed: ${e?.message || e}`));
    return chain;
  }

  async function start() {
    if (started) return api; started = true;
    logFn(`[l3fleet] up · ${fixed.length} static shard(s)${fixed.length ? ' (' + fixed.join(', ') + ')' : ''}${url ? ` · following ${url}/v1/l3/markets every ${Math.round(pollMs / 1000)} s` : ' · no engine to follow'}${byOutcome ? ' · shards by outcome' : ''}`);
    // the sweep's own timer starts BEFORE the first reconcile, which can run for hours on a large fleet; it is
    // self-guarding (it does nothing until the engine's listing is known) and judges by intent, not by what is up yet
    if (journalDir && journalSweepMs > 0) { jtimer = setInterval(sweepJournals, journalSweepMs); jtimer.unref?.(); }
    await reconcile();
    if (url && pollMs > 0) { timer = setInterval(() => { reconcile(); }, pollMs); timer.unref?.(); }
    if (journalDir) sweepJournals();
    return api;
  }
  async function stop() {
    stopping = true;
    if (timer) { clearInterval(timer); timer = null; }
    if (jtimer) { clearInterval(jtimer); jtimer = null; }
    try { await chain; } catch {}
    for (const s of [...miners.keys()]) await stopShard(s, 'shutting down');
    if (server) { try { server.close(); } catch {} server = null; }
  }

  // ------------------------------------------------------------- views
  /// one shard's standing. `ok` is what a 503 is about: stalled, disagreeing NOW, halted, or votes the log keeps
  /// refusing. The dissent test used to be the cumulative counter — `dissents === 0` — which made one dissent, ever,
  /// mark the shard unhealthy for the life of the process. After the volumes filled on 2026-11-11 every shard that
  /// had replayed a truncated journal carried one, so the fleet could never report healthy again however well it was
  /// running. What matters is whether this miner disagrees with the sequencer about the batch it just processed; the
  /// count is still reported, it just is not a verdict.
  function shardStatus(shard, e) {
    const s = e.miner.status();
    const voteBacklog = Math.max(0, Number(s.voteFailures || 0) - Number(s.voteRetried || 0));
    const halted = e.miner.quorum ? !!e.miner.quorum.halted : false;
    const disagreeing = !!(s.lastVote && s.lastVote.ok === false);
    const ok = !s.stalled && !disagreeing && !halted && voteBacklog === 0;
    // WHY, not just that. A halted shard used to report `halted: true` and a dissent count, and the reason —
    // which decides the remedy, because "roots differ" has four quite different causes — existed only as a log
    // line on the machine. On 2026-11-22 that cost a day of guessing about six live shards.
    // the miner's own words, not a guess: a vote's `why` distinguishes a signature or domain problem from a
    // genuine disagreement about the book, and those want opposite remedies
    const why = s.lastVote && s.lastVote.ok === false ? String(s.lastVote.why || 'no reason recorded') : null;
    const q = halted && e.miner.quorum ? e.miner.quorum.status() : null;
    const fork = q && q.lastFork ? { index: q.lastFork.index, sticky: !!q.lastFork.sticky, forks: q.forks } : null;
    return { shard, ok, source: e.source, since: e.since, index: s.index, epoch: s.epoch, offset: s.offset, stalled: s.stalled, batches: s.batches, votes: s.votes, dissents: s.dissents, disagreeing, why, fork,
             resting: s.resting, lagSeconds: s.lagSeconds, votesPending: s.votesPending, voteBacklog, journalBytes: s.journalBytes ?? null, journalCapped: s.journalCapped ?? null,
             finalIndex: e.miner.quorum ? e.miner.quorum.finalIndex : null, halted, errors: s.errors };
  }
  function status() {
    // A fleet that has not finished bringing its shards up is STARTING, not broken. `ok` was `ready`, which only
    // turns true after the first reconcile — and a reconcile starts shards one at a time, each replaying its journal
    // op by op, so on 101 shards it runs for over an hour. Every `fly deploy` of a miner therefore failed its health
    // check ("timeout reached waiting for health checks to pass") while the machine underneath was perfectly fine,
    // which is how the fleet ended up on three different images. Not started, or an engine we have never reached, is
    // still a 503: then we genuinely do not know what we are supposed to be mining.
    const up = started && (!url || stats.lastPollOk > 0);
    const shards = {}; let ok = up;
    for (const [shard, e] of miners) { const st = shardStatus(shard, e); shards[shard] = st; if (!st.ok) ok = false; }
    return { ok, starting: up && !ready, miner: address, shards, count: miners.size, static: fixed, assign: { ...astats }, engine: url ? { url, listed: stats.listed, lastPoll: stats.lastPoll, lastPollOk: stats.lastPollOk, polls: stats.polls, pollErrors: stats.pollErrors, lastPollError: stats.lastPollError, pollMs } : null,
             started: stats.started, stopped: stats.stopped, startFailures: stats.startFailures, ready, uptime: process.uptime(),
             journals: journalDir ? { dir: journalDir, retainDays: journalRetainDays, ...jstats } : null };
  }
  const healthy = () => status().ok;
  /// run the journal sweep now (the timer calls it every journalSweepMs); returns its counters
  const sweepNow = () => sweepJournals();

  /// Prometheus text: miner.js's series with a shard label, from each miner's status(), plus the fleet's own
  function metricsText() {
    const lines = []; const seen = new Set();
    const g = (k, labels, val, help) => { if (!seen.has(k)) { lines.push(`# HELP rolla_l3_${k} ${help}`, `# TYPE rolla_l3_${k} gauge`); seen.add(k); } lines.push(`rolla_l3_${k}{${labels}} ${val}`); };
    const me = `miner="${String(address).toLowerCase()}"`;
    for (const [shard, e] of miners) {
      const s = e.miner.status(); const l = `shard="${shard}",${me}`;
      g('batches_total', l, s.batches, 'batches this miner replayed');
      g('votes_total', l, s.votes, 'batches this miner agreed with');
      g('dissents_total', l, s.dissents, 'batches this miner disagreed with');
      g('bad_signatures_total', l, s.badSigs, 'orders whose signature the miner refused');
      g('unchecked_grants_total', l, s.unchecked, 'session-key orders accepted without a grant check (no RPC)');
      g('orders_total', l, s.orders, 'order signatures verified');
      g('fills_total', l, s.fills, 'fills the miner matched');
      g('gaps_total', l, s.gaps, 'gaps seen in the log');
      g('errors_total', l, s.errors, 'batches that threw');
      g('index', l, s.index, 'the last batch index applied');
      g('epoch', l, s.epoch, 'the current epoch');
      g('offset', l, s.offset, 'the last log offset applied');
      g('resting_orders', l, s.resting, 'resting orders in the shard');
      g('lag_seconds', l, Number(s.lagSeconds).toFixed?.(1) ?? s.lagSeconds, 'seconds since the last batch (-1: none yet)');
      g('verify_ms_per_order', l, s.perOrderMs, 'milliseconds of signature verification per order');
      g('verify_ms_total', l, Math.round(s.verifyMs), 'wall milliseconds spent verifying signatures');
      g('root_ms_total', l, Math.round(s.rootMs), 'wall milliseconds spent on Merkle roots and book commitments');
      g('apply_ms_total', l, Math.round(s.applyMs), 'wall milliseconds spent replaying ops into the book');
      g('commit_ms_total', l, Math.round(s.commitMs), 'wall milliseconds spent feeding the incremental state commitment (L3_COMMIT=tree)');
      g('votes_pending', l, s.votesPending, 'votes not yet in the log');
      g('stalled', l, s.stalled ? 1 : 0, '1 when the miner stopped applying batches');
      g('final_index', l, e.miner.quorum ? e.miner.quorum.finalIndex : -1, 'the last batch this miner sees as final');
      g('halted', l, e.miner.quorum && e.miner.quorum.halted ? 1 : 0, '1 when this miner sees a fork');
      g('rewards_micro', l, s.rewards?.accrued ?? 0, 'microRolla accrued in the open epoch, not yet in a closed rewardsRoot');
    }
    g('fleet_shards', me, miners.size, 'shards this process is mining');
    g('fleet_static_shards', me, fixed.length, 'shards pinned by L3_SHARDS');
    g('fleet_listed_shards', me, stats.listed.length, 'shards the engine lists at /v1/l3/markets');
    g('fleet_polls_total', me, stats.polls, 'engine polls');
    g('fleet_poll_errors_total', me, stats.pollErrors, 'engine polls that failed');
    g('fleet_last_poll_ok_seconds', me, stats.lastPollOk ? Math.round((Date.now() - stats.lastPollOk) / 1000) : -1, 'seconds since the engine last answered');
    g('fleet_started_total', me, stats.started, 'miners started');
    g('fleet_stopped_total', me, stats.stopped, 'miners stopped');
    g('fleet_start_failures_total', me, stats.startFailures, 'miners that failed to start');
    g('fleet_ready', me, ready ? 1 : 0, '1 once the first reconcile completed');
    g('up', me, 1, 'process up');
    return lines.join('\n') + '\n';
  }

  let server = null;
  /// /healthz (200 while every shard is sound, 503 otherwise) and /metrics for the whole fleet
  function serve(port = 8080, host = '0.0.0.0') {
    server = http.createServer((req, res) => {
      if (req.url === '/healthz' || req.url === '/') {
        const s = status();
        res.writeHead(s.ok ? 200 : 503, { 'content-type': 'application/json' });
        return res.end(JSON.stringify(s));
      }
      if (req.url === '/metrics') { res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' }); return res.end(metricsText()); }
      res.writeHead(404); res.end();
    });
    server.listen(port, host, () => logFn(`[l3fleet] health on ${host}:${port}`));
    return server;
  }

  const api = { start, stop, reconcile, status, healthy, metricsText, serve, shards: () => [...miners.keys()], minerOf: (s) => miners.get(String(s))?.miner || null, sweepJournals: sweepNow, stats, get ready() { return ready; } };
  return api;
}
