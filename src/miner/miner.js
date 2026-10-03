// engine/l3/miner/miner.js — an L3 miner: it consumes a shard's sequenced batches, replays them into ITS OWN
// book, recomputes the roots, and publishes a signed vote or a signed dissent.
//
// The miner is the decentralisation (L3-SIDECHAIN.md §4). It trusts nothing it is told:
//   * it recomputes every order's EIP-712 hash from the order's own fields, so a batch that merely CLAIMS a
//     hash proves nothing;
//   * it recovers every signature itself (verify.js's worker pool — docs/L3-MINERS.md §6: this is the whole
//     per-order cost of the venue), and refuses an order whose signer is neither the user nor an authorised
//     session key;
//   * it recomputes ordersRoot from the ops in the order the batch lists them, so a reordering is visible;
//   * it MATCHES — the batch does not contain the fills, by design, so the only way to produce a fillsRoot is
//     to run a book. A miner that did not match cannot vote;
//   * it recomputes batchRoot and compares with the sequencer's. Equal → a vote. Different → a dissent that
//     carries the miner's own roots, so the disagreement is localisable (different ordersRoot = different
//     orders; same ordersRoot, different fillsRoot = the books disagree about matching).
//
// Its book is the JS matcher by default and the native book with L3_NATIVE=1 (one bookd process per market and
// outcome, its own journal) — the two agree fill for fill and produce the same transcript hash
// (native/book/diff.mjs), so a disagreement between the two IMPLEMENTATIONS would show up as a dissent instead
// of hiding inside one of them. That is the strongest argument for the native backend being available here.
//
// Durability: the miner keeps its own copy of the log (`<shard>.batches.jsonl`) and of everything it signed
// (`<shard>.votes.jsonl`). That is data availability — every validator holds the log (§3 of the sidechain doc)
// — and it is how a restart catches up without asking the sequencer for anything: replay the local journal,
// then resume the subscription at lastOffset+1.
//
// Observability: /healthz and /metrics in the shape engine/rolla-node.mjs established.
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { createBook } from '../matcher.js';
import { parseL3Order, BOOK_DOMAIN } from '../desk.js';
import { fillsRootOf, ordersRootOf, bookHashOf, batchRootOf, voteDigestOf, merkleProof, fillLeaf, ZERO32 } from './merkle.js';
import { createVerifier, recoverDigestSigner, signDigest } from './verify.js';
import { createQuorum } from './quorum.js';
import { rewardsFromEnv } from './rewards.js';
import { ordersTopic, votesTopic } from './log.js';

const lower = (a) => String(a || '').toLowerCase();

// ---------------------------------------------------------------------------------------- the shard's books
/// The deterministic state machine a batch is applied to — the SAME code in the sequencer and in every miner,
/// which is the only way two processes can be expected to agree. A shard holds one book per (market, outcome):
/// books never share state, so a shard with many markets needs no coordination inside itself.
///
/// `seq` is the shard's op counter, not a book's matcher sequence: each book numbers its own sequence, so the
/// shard needs a counter of its own for the batch's seqFrom/seqTo. It counts applied ops, nothing else, so it is
/// identical on every replica.
export function createShardState({ newBook = null, logger = null } = {}) {
  const make = newBook || (() => createBook());
  const books = new Map();     // `${market}:${outcome}` → book
  let seq = 0, applied = 0, fills = 0, cancels = 0, rejects = 0;
  const keyOf = (market, outcome) => `${Number(market)}:${Number(outcome)}`;
  function bookFor(market, outcome) {
    const key = keyOf(market, outcome); let b = books.get(key);
    if (!b) { b = make(key); books.set(key, b); }
    return b;
  }
  /// apply one op. Returns the matcher's result for an add, { fills: [] } for a cancel.
  function apply(op) {
    seq++; applied++;
    if (op.t === 'cancel') {
      const b = books.get(keyOf(op.market, op.outcome));
      const o = b ? b.cancel(op.hash) : null;
      if (o) cancels++; else rejects++;
      return { fills: [], cancelled: !!o, remaining: o ? o.remaining : 0n, reason: o ? null : 'unknown order' };
    }
    const o = parseL3Order(op.order);
    const b = bookFor(o.marketId, o.outcome);
    const r = b.add({ hash: op.hash, user: lower(o.user), buy: o.buy, price: o.price, size: o.size, postOnly: o.postOnly, ioc: o.ioc, ts: op.at || 0 });
    fills += r.fills.length; if (r.reason) rejects++;
    return r;
  }
  /// every resting order in the shard, for the epoch-boundary book commitment
  function resting() { const out = []; for (const b of books.values()) for (const o of b.orders()) out.push(o); return out; }
  return {
    apply, bookFor, books, resting,
    bookHash: () => bookHashOf(resting()),
    get seq() { return seq; },
    get size() { let n = 0; for (const b of books.values()) n += b.size; return n; },
    stat: () => ({ seq, applied, fills, cancels, rejects, books: books.size, resting: (() => { let n = 0; for (const b of books.values()) n += b.size; return n; })() }),
    close() { for (const b of books.values()) { try { b.close?.(); } catch (e) { logger && logger(`[l3miner] book close failed: ${e.message}`); } } books.clear(); },
  };
}

/// the book factory the environment asks for. L3_NATIVE=1 uses native/book/bookd (one process per market and
/// outcome, journalled beside the miner's own); the client is imported lazily because it spawns a process and
/// needs the built binary, which a JS-only miner must not require. Note the one deliberate semantic difference
/// (L3-NATIVE-BOOK.md §3.7): the native book is a tick grid and REFUSES off-tick prices instead of rounding
/// them, so a miner may only run it for a shard whose prices are multiples of L3_TICK_SIZE. Running it for any
/// other market would produce honest dissents on every batch.
export async function bookFactory({ env = process.env, dir = null, logger = null } = {}) {
  if (env.L3_NATIVE !== '1') return () => createBook();
  const { createNativeBook } = await import('../native.js');
  const tickSize = BigInt(env.L3_TICK_SIZE || 10n ** 14n);
  return (key) => createNativeBook({ journal: dir ? path.join(dir, `${key.replace(':', '-')}.book`) : null, tickSize, log: logger });
}

// ------------------------------------------------------------------------------------------------ the miner
/**
 * createMiner({ shard, log, account, ... })
 *   shard       the shard id, e.g. the market id as a string
 *   log         a Log (log.js) — the replicated log, NOT a printer (`logger` is the printer)
 *   account     a viem account (its key never leaves the process; only the address is ever printed)
 *   domain      the RollaBook EIP-712 domain the orders were signed under (BOOK_DOMAIN(chainId, book))
 *   dir         where the journal lives
 *   newBook     book factory (default the JS matcher)
 *   verifier    a verifier (default one of its own, L3_VERIFY_WORKERS workers)
 *   sequencers  allowlist of sequencer addresses, or null for "any signature"
 *   grantOf     optional async (user, key) => { expiry, used, max } — without it, an order signed by a key
 *               other than the user is accepted on the sequencer's word and counted in `unchecked`
 *   watchVotes  also track every miner's votes (its own included), so the miner knows what is final — and what
 *               it has earned in µROLLA (rewards.js) — without asking the engine or the operator
 *   tamper      TEST HOOK: (batch, fills) => fills — a Byzantine miner. Never set in production.
 */
export function createMiner({
  shard, log, account, domain = null, dir = null, newBook = null, verifier = null, workers = null,
  sequencers = null, grantOf = null, watchVotes = true, threshold = 2, staleMs = 30000,
  tamper = null, onBatch = null, onVote = null, logger: logFn = console.log, env = process.env,
} = {}) {
  if (!shard) throw new Error('a miner needs a shard');
  if (!log) throw new Error('a miner needs a log');
  if (!account?.address) throw new Error('a miner needs an account');
  const address = lower(account.address);
  const dom = domain || BOOK_DOMAIN(Number(env.CHAIN_ID || 46630), env.PREDICT_BOOK || '0x' + '00'.repeat(20));
  const allowSeq = sequencers ? new Set([...sequencers].map(lower)) : null;
  let make = newBook || (() => createBook());          // replaced by the native factory in start() when asked
  let state = createShardState({ newBook: make, logger: logFn });
  const v = verifier || createVerifier({ domain: dom, workers: workers == null ? (env.L3_VERIFY_WORKERS != null ? Number(env.L3_VERIFY_WORKERS) : null) : workers, logger: logFn });
  const ownVerifier = !verifier;
  const orders = ordersTopic(shard), votes = votesTopic(shard);
  const rewards = watchVotes ? rewardsFromEnv(env) : null;
  const quorum = watchVotes ? createQuorum({ threshold, rewards, logger: logFn }) : null;

  let jBatches = null, jVotes = null;
  if (dir) { fs.mkdirSync(dir, { recursive: true }); jBatches = path.join(dir, `${shard}.batches.jsonl`); jVotes = path.join(dir, `${shard}.votes.jsonl`); }
  const append = (file, rec) => { if (file) try { fs.appendFileSync(file, JSON.stringify(rec) + '\n'); } catch (e) { logFn(`[l3miner] journal append failed: ${e.message}`); } };

  const m = { batches: 0, votes: 0, dissents: 0, badSigs: 0, unchecked: 0, gaps: 0, replayed: 0, verifyMs: 0, applyMs: 0, rootMs: 0, orders: 0, fills: 0, lastAt: 0, lastOffset: -1, errors: 0, voteFailures: 0, voteRetried: 0 };
  let index = -1, epoch = 0, prevRoot = ZERO32, offset = -1, stalled = null, subOrders = null, subVotes = null, started = false, stopping = false;
  let lastVote = null, lastBookHash = ZERO32, inflight = Promise.resolve();
  const recent = new Map();                            // index → { batch, fills }, for challenge proofs
  const keepFills = Number(env.L3_KEEP_FILLS || 64);

  // ------------------------------------------------------------- the work, for one batch
  /// verify a batch's order signatures. Returns { ok, why, signers } — `ok` false means the batch itself is
  /// invalid (the sequencer admitted something the chain would refuse), which is a dissent, not a crash.
  async function verifyOps(ops) {
    const items = [], at = [];
    for (let i = 0; i < ops.length; i++) if (ops[i].t !== 'cancel') { items.push({ order: ops[i].order, signature: ops[i].sig }); at.push(i); }
    if (!items.length) return { ok: true, why: null };
    const t0 = process.hrtime.bigint();
    const out = await v.verifyOrders(items);
    m.verifyMs += Number(process.hrtime.bigint() - t0) / 1e6; m.orders += items.length;
    for (let k = 0; k < out.length; k++) {
      const op = ops[at[k]], r = out[k];
      if (!r) { m.badSigs++; return { ok: false, why: `op ${at[k]}: signature does not recover` }; }
      if (r.hash !== op.hash) { m.badSigs++; return { ok: false, why: `op ${at[k]}: the batch's order hash is not the EIP-712 hash of the order` }; }
      const user = lower(op.order?.user);
      if (r.signer !== user) {
        // a session key: the desk's grant decides, and only an RPC can read it
        if (op.signer && lower(op.signer) !== r.signer) { m.badSigs++; return { ok: false, why: `op ${at[k]}: the batch names a different signer` }; }
        if (grantOf) {
          let g = null; try { g = await grantOf(user, r.signer); } catch { g = null; }
          if (!g || Number(g.expiry) * 1000 <= (op.at || Date.now())) { m.badSigs++; return { ok: false, why: `op ${at[k]}: no live session grant for ${r.signer}` }; }
        } else m.unchecked++;
      }
    }
    return { ok: true, why: null };
  }

  async function handleBatch({ offset: off, value: batch }) {
    if (stopping) return;
    if (String(batch.shard) !== String(shard)) return;                       // not ours (a shared topic)
    if (Number(batch.index) <= index) return;                                // already applied: at-least-once
    if (stalled) return;
    try {
      // the sequencer's own signature over the root it claims
      if (allowSeq) {
        const who = recoverDigestSigner(batch.batchRoot, batch.sig);
        if (!who || !allowSeq.has(who)) { stalled = `batch ${batch.index} is not signed by a known sequencer`; logFn(`[l3miner] ${stalled}`); return; }
      }
      if (index >= 0 && Number(batch.index) !== index + 1) { m.gaps++; stalled = `the log skipped from ${index} to ${batch.index}`; logFn(`[l3miner] ${stalled}`); return; }
      const chainOk = (batch.prevRoot || ZERO32) === prevRoot;

      const ops = Array.isArray(batch.ops) ? batch.ops : [];
      const sigs = await verifyOps(ops);

      const t1 = process.hrtime.bigint();
      let fills = [];
      for (const op of ops) { const r = state.apply(op); if (r.fills?.length) fills.push(...r.fills); }
      m.applyMs += Number(process.hrtime.bigint() - t1) / 1e6; m.fills += fills.length;
      if (tamper) fills = tamper(batch, fills);                               // TEST HOOK: a Byzantine miner

      const t2 = process.hrtime.bigint();
      const ordersRoot = ordersRootOf(ops);
      const fillsRoot = fillsRootOf(fills);
      const bookHash = (batch.bookHash && batch.bookHash !== ZERO32) ? state.bookHash() : ZERO32;
      const mine = {
        shard: String(shard), epoch: Number(batch.epoch), index: Number(batch.index),
        seqFrom: Number(batch.seqFrom), seqTo: Number(batch.seqTo),
        prevRoot: batch.prevRoot || ZERO32, ordersRoot, fillsRoot, bookHash,
      };
      const myRoot = batchRootOf(mine);
      m.rootMs += Number(process.hrtime.bigint() - t2) / 1e6;

      const ok = myRoot === batch.batchRoot && chainOk && sigs.ok;
      const why = !sigs.ok ? sigs.why : !chainOk ? `prevRoot ${batch.prevRoot} does not follow ${prevRoot}` : myRoot !== batch.batchRoot ? 'roots differ' : null;
      const vote = {
        shard: String(shard), epoch: mine.epoch, index: mine.index, claimed: batch.batchRoot,
        batchRoot: myRoot, ordersRoot, fillsRoot, bookHash, seqTo: mine.seqTo,
        fills: fills.length, ops: ops.length, ok, why, at: Date.now(), miner: address, sig: '0x',
      };
      vote.sig = await signDigest(account, voteDigestOf(vote));

      index = mine.index; prevRoot = batch.batchRoot || myRoot; epoch = mine.epoch; offset = off;
      if (bookHash !== ZERO32) lastBookHash = bookHash;
      m.batches++; m.lastAt = Date.now(); m.lastOffset = off;
      if (ok) m.votes++;
      else { m.dissents++; logFn(`[l3miner] DISSENT on ${shard}#${mine.index}: ${why}${myRoot !== batch.batchRoot ? ` (mine ${myRoot.slice(0, 12)}…, theirs ${String(batch.batchRoot).slice(0, 12)}…)` : ' (the roots agree: the batch itself is invalid)'}`); }

      append(jBatches, { offset: off, index: mine.index, epoch: mine.epoch, batch });   // the miner's own copy of the log
      append(jVotes, vote);
      recent.set(mine.index, { batch, fills });
      if (recent.size > keepFills) { const oldest = Math.min(...recent.keys()); recent.delete(oldest); }
      lastVote = vote;
      // the vote joins the append pipeline: the next batch is verified while this vote is in flight (a vote
      // used to cost one log round trip per batch — on RunPod the miners fell 12 s behind a sequencer they
      // could verify 10× faster than it fed them, with the broker 100–300 ms away)
      queueVote(vote, () => {
        quorum && quorum.announce(batch);
        onBatch && onBatch({ batch, vote, fills, ok });
        onVote && onVote(vote);
      });
    } catch (e) {
      m.errors++; logFn(`[l3miner] batch ${batch?.index} failed: ${e.message}`);
      stalled = `batch ${batch?.index}: ${e.message}`;
    }
  }
  // ------------------------------------------------------------- the vote pipeline
  /// votes go out in index order, as many per produce request as are waiting; a refusing log keeps them (in
  /// order) and retries — the miner keeps verifying meanwhile, and its health says votes are waiting
  const votesPending = [], votesRetry = []; let votePump = null, voteRetryTimer = null;
  function queueVote(vote, after) { votesPending.push({ vote, after }); pumpVotes(); }
  function pumpVotes() {
    if (!votePump) votePump = (async () => { try { await pumpVotesLoop(); } catch (e) { logFn(`[l3miner] vote pipeline: ${e.message}`); } finally { votePump = null; if (votesPending.length) pumpVotes(); } })();
    return votePump;
  }
  async function sendVotes(group) {
    if (group.length > 1 && typeof log.appendMany === 'function') await log.appendMany(votes, group.map((g) => g.vote));
    else for (const g of group) await log.append(votes, g.vote);
  }
  async function pumpVotesLoop() {
    while (votesPending.length) {
      if (votesRetry.length) await drainVotes();
      const group = votesPending.splice(0, 500);
      if (votesRetry.length) { votesRetry.push(...group); m.voteFailures += group.length; continue; }   // still refusing: wait behind the earlier ones
      try { await sendVotes(group); for (const g of group) runAfter(g); }
      catch (e) { votesRetry.push(...group); m.voteFailures += group.length; logFn(`[l3miner] ${group.length} vote(s) could not be logged (${e.message}); ${votesRetry.length} waiting`); armVoteRetry(); }
    }
  }
  async function drainVotes() {
    while (votesRetry.length) {
      const g = votesRetry[0];
      try { await log.append(votes, g.vote); votesRetry.shift(); m.voteRetried++; runAfter(g); } catch { armVoteRetry(); return; }
    }
  }
  function armVoteRetry() { if (voteRetryTimer || stopping) return; voteRetryTimer = setTimeout(() => { voteRetryTimer = null; pumpVotes(); }, 1000); voteRetryTimer.unref?.(); }
  const runAfter = (g) => { try { g.after && g.after(); } catch (e) { logFn(`[l3miner] after-vote hook failed: ${e.message}`); } };
  const votesSettled = async () => { while (votesPending.length || votePump) await (votePump || pumpVotes()); };

  /// handlers are serialised: the log may deliver quickly, but a book is a state machine and two batches must
  /// never be in flight at once
  const serial = (fn) => (rec) => (inflight = inflight.then(() => fn(rec)).catch((e) => { m.errors++; logFn(`[l3miner] ${e.message}`); }));

  // ------------------------------------------------------------- journal replay
  /// replay the local copy of the log. Roots are recomputed and compared with what the miner signed at the
  /// time: a journal that does not reproduce its own votes is corrupt, and the miner says so and starts from
  /// the log at offset 0 instead. Signatures are NOT re-verified (5.6 ms each, on the miner's own disk) unless
  /// L3_VERIFY_REPLAY=1.
  function replayJournal() {
    if (!jBatches || !fs.existsSync(jBatches)) return { ok: true, replayed: 0 };
    let lines = [];
    try { lines = fs.readFileSync(jBatches, 'utf8').split('\n').filter(Boolean); } catch { return { ok: true, replayed: 0 }; }
    let n = 0;
    for (const line of lines) {
      let rec; try { rec = JSON.parse(line); } catch { continue; }
      const batch = rec.batch; if (!batch || Number(batch.index) <= index) continue;
      if (index >= 0 && Number(batch.index) !== index + 1) return { ok: false, why: `journal skips ${index} → ${batch.index}`, replayed: n };
      const ops = Array.isArray(batch.ops) ? batch.ops : [];
      let fills = [];
      for (const op of ops) { const r = state.apply(op); if (r.fills?.length) fills.push(...r.fills); }
      const bookHash = (batch.bookHash && batch.bookHash !== ZERO32) ? state.bookHash() : ZERO32;
      const myRoot = batchRootOf({ shard: String(shard), epoch: Number(batch.epoch), index: Number(batch.index), seqFrom: Number(batch.seqFrom), seqTo: Number(batch.seqTo), prevRoot: batch.prevRoot || ZERO32, ordersRoot: ordersRootOf(ops), fillsRoot: fillsRootOf(fills), bookHash });
      if (myRoot !== batch.batchRoot) return { ok: false, why: `journal batch ${batch.index} no longer reproduces its root`, replayed: n };
      index = Number(batch.index); epoch = Number(batch.epoch); prevRoot = batch.batchRoot; offset = Number(rec.offset);
      if (bookHash !== ZERO32) lastBookHash = bookHash;
      n++; m.replayed++;
    }
    return { ok: true, replayed: n };
  }

  // ------------------------------------------------------------- lifecycle
  async function start() {
    if (started) return api; started = true;
    if (!newBook && env.L3_NATIVE === '1') {            // the native backend, resolved before anything is applied
      make = await bookFactory({ env, dir, logger: logFn });
      state.close(); state = createShardState({ newBook: make, logger: logFn });
    }
    const r = replayJournal();
    if (!r.ok) {
      logFn(`[l3miner] ${r.why} — discarding the local journal and replaying ${shard} from the log`);
      try { fs.renameSync(jBatches, `${jBatches}.corrupt.${Date.now()}`); } catch {}
      state.close(); index = -1; epoch = 0; prevRoot = ZERO32; offset = -1; lastBookHash = ZERO32;
      state = createShardState({ newBook: make, logger: logFn });
    } else if (r.replayed) {
      logFn(`[l3miner] ${shard}: replayed ${r.replayed} batch(es) from the journal → index ${index}, ${state.size} resting, book ${lastBookHash.slice(0, 12)}…`);
    }
    const from = offset + 1;
    logFn(`[l3miner] up · shard ${shard} · miner ${account.address} · log ${log.kind} · ${v.workers} verify worker(s) · from offset ${from}`);
    subOrders = await log.subscribe(orders, from, serial(handleBatch));
    // its OWN votes go in too: the agreeing set is what earns, so a miner that skipped itself could not account
    // for its own µROLLA (rewards.js), and the quorum keys votes by miner so re-reading one is harmless
    if (quorum) subVotes = await log.subscribe(votes, 0, serial(async ({ value }) => { await quorum.vote(value); }));
    return api;
  }
  async function stop() {
    stopping = true;
    try { await inflight; } catch {}
    try { await votesSettled(); } catch {}
    if (voteRetryTimer) { clearTimeout(voteRetryTimer); voteRetryTimer = null; }
    try { subOrders && subOrders.close && (await subOrders.close()); } catch {}
    try { subVotes && subVotes.close && (await subVotes.close()); } catch {}
    if (ownVerifier) { try { await v.close(); } catch {} }
    state.close();
    if (server) { try { server.close(); } catch {} }
  }

  // ------------------------------------------------------------- views
  const lagSeconds = () => (m.lastAt ? (Date.now() - m.lastAt) / 1000 : -1);
  const healthy = () => !stalled && m.dissents === 0 && votesRetry.length === 0 && (m.lastAt === 0 || lagSeconds() * 1000 < staleMs * 20);
  function status() {
    return {
      shard: String(shard), miner: account.address, log: log.kind, index, epoch, offset, stalled,
      resting: state.size, bookHash: lastBookHash, prevRoot,
      lagSeconds: lagSeconds(), workers: v.workers, votesPending: votesPending.length + votesRetry.length, ...m,
      perOrderMs: m.orders ? Number((m.verifyMs / m.orders).toFixed(3)) : 0,
      lastVote: lastVote ? { index: lastVote.index, ok: lastVote.ok, fillsRoot: lastVote.fillsRoot, batchRoot: lastVote.batchRoot } : null,
      quorum: quorum ? quorum.status() : null,
      rewards: rewards ? { accrued: rewards.accruedOf(address).toString(), debt: rewards.debtOf(address).toString(), lifetime: rewards.state().lifetime[address] || null } : null,
      state: state.stat(),
    };
  }
  /// a Merkle proof of one fill against a batch's fillsRoot — the challenge proof of docs/L3-MINERS.md §5.
  /// The fills come from the miner's own matching of the last `L3_KEEP_FILLS` batches; a batch cannot be
  /// re-matched in isolation (a book has a history), which is exactly why the miner keeps them.
  function proveFill(index, predicate) {
    const r = recent.get(Number(index));
    if (!r) return null;
    const i = r.fills.findIndex(predicate);
    if (i < 0) return null;
    const leaves = r.fills.map(fillLeaf);
    return { fill: r.fills[i], proof: merkleProof(leaves, i), fillsRoot: r.batch.fillsRoot, batchRoot: r.batch.batchRoot, index: Number(index), fills: r.fills.length };
  }

  let server = null;
  /// /healthz and /metrics, in the shape engine/rolla-node.mjs established
  function serve(port = Number(env.PORT || 8090), host = '0.0.0.0') {
    server = http.createServer((req, res) => {
      if (req.url === '/healthz') {
        const ok = healthy(); const s = status();
        res.writeHead(ok ? 200 : 503, { 'content-type': 'application/json' });
        return res.end(JSON.stringify({ ok, miner: account.address, shard: String(shard), index, epoch, stalled, dissents: m.dissents, lagSeconds: s.lagSeconds, resting: s.resting, uptime: process.uptime() }));
      }
      if (req.url === '/metrics') {
        const s = status(); const lines = [];
        const g = (k, val, help) => { lines.push(`# HELP rolla_l3_${k} ${help}`, `# TYPE rolla_l3_${k} gauge`, `rolla_l3_${k}{shard="${shard}",miner="${address}"} ${val}`); };
        g('batches_total', m.batches, 'batches this miner replayed');
        g('votes_total', m.votes, 'batches this miner agreed with');
        g('dissents_total', m.dissents, 'batches this miner disagreed with');
        g('bad_signatures_total', m.badSigs, 'orders whose signature the miner refused');
        g('unchecked_grants_total', m.unchecked, 'session-key orders accepted without a grant check (no RPC)');
        g('orders_total', m.orders, 'order signatures verified');
        g('fills_total', m.fills, 'fills the miner matched');
        g('gaps_total', m.gaps, 'gaps seen in the log');
        g('errors_total', m.errors, 'batches that threw');
        g('index', index, 'the last batch index applied');
        g('epoch', epoch, 'the current epoch');
        g('offset', offset, 'the last log offset applied');
        g('resting_orders', s.resting, 'resting orders in the shard');
        g('lag_seconds', s.lagSeconds.toFixed?.(1) ?? s.lagSeconds, 'seconds since the last batch');
        g('verify_ms_per_order', s.perOrderMs, 'milliseconds of signature verification per order');
        g('verify_workers', v.workers, 'signature verification worker threads');
        g('stalled', stalled ? 1 : 0, '1 when the miner stopped applying batches');
        g('final_index', quorum ? quorum.finalIndex : -1, 'the last batch this miner sees as final');
        g('rewards_micro', rewards ? rewards.accruedOf(address) : 0, 'microRolla accrued in the open epoch, not yet in a closed rewardsRoot');
        g('rewards_credited_micro', rewards ? (rewards.state().lifetime[address]?.credited || 0) : 0, 'microRolla credited since this process started');
        g('rewards_bounty_micro', rewards ? (rewards.state().lifetime[address]?.bounty || 0) : 0, 'microRolla earned from dissents proven right');
        g('rewards_slashed_micro', rewards ? (rewards.state().lifetime[address]?.slashed || 0) : 0, 'microRolla slashed');
        g('rewards_debt_micro', rewards ? rewards.debtOf(address) : 0, 'microRolla owed from a slash the open epoch could not cover');
        g('halted', quorum && quorum.halted ? 1 : 0, '1 when this miner sees a fork');
        g('up', 1, 'process up');
        res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' });
        return res.end(lines.join('\n') + '\n');
      }
      res.writeHead(404); res.end();
    });
    server.listen(port, host, () => logFn(`[l3miner] health on ${host}:${port}`));
    return server;
  }

  const api = { shard: String(shard), address: account.address, start, stop, status, serve, proveFill, state, quorum, rewards, verifier: v,
                get index() { return index; }, get epoch() { return epoch; }, get bookHash() { return lastBookHash; }, get stalled() { return stalled; },
                get lastVote() { return lastVote; }, metrics: () => ({ ...m }) };
  return api;
}
