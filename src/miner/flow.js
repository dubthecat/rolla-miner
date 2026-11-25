// engine/l3/miner/flow.js — the venue's own order flow, as a generator, for load tests that mean something.
//
// WHY THIS REPLACES A NOISE GENERATOR. Every scale run so far fired random orders at a random offset from a fixed
// mid. That measures the sequencer and the miners honestly enough, and it measures nothing about the venue,
// because the book it builds is not a book: the resting set spreads evenly instead of clustering at the touch,
// nothing is ever re-quoted because no fair ever moves, no maker is ever picked off, and the epoch commitment
// hashes a shape the live venue never has. The numbers that come out are real numbers about a fictional market.
//
// So this generates the flow the venue actually has, out of the venue's own parts:
//
//   · MAKERS quote with lib/quote-math.js — the SAME function the live market maker calls (engine/mm/strategies/
//     l3-quoter.js re-exports it). Half-spread from the fair's band × bandK with its floor and its relative cap,
//     inventory skew, tick rounding, the post-only pull one tick inside somebody else's resting order, the floor
//     quote for a market nobody can price, the pull before close. If the spread rule changes, this changes with
//     it, in the same commit, which is the whole reason the arithmetic was lifted into its own module.
//   · The FAIR MOVES. A logit random walk per outcome, softmaxed back onto the simplex so the outcomes of one
//     market still sum to 1 — which is what makes a move in outcome 0 re-quote outcome 1, exactly as it does
//     live. Requoting is most of a maker's op count and a static mid hides all of it.
//   · TAKERS cross the touch, and some of them are INFORMED: they see the next step of the walk before the maker
//     has re-quoted and take the side it favours. That is adverse selection, it is where the maker's inventory
//     comes from, and inventory is what skews the next quote. The loop closes: a fill frees a level, a freed
//     level is re-quoted at a skewed price, the skew moves the touch, the next taker trades against that.
//
// FILLS COME FROM THE MATCHER, NOT FROM A MODEL. `sync(orders)` is handed the shard's real resting orders; any
// tracked quote that is gone was filled or cancelled, and any that shrank was partly filled. The position that
// skews the quotes is therefore the position the book actually gave the maker. A generator that guessed its own
// fills would drift away from the venue it is supposed to be testing, silently, and the drift would look like a
// result.
//
// BOUNDED BY CONSTRUCTION. The resting set is makers × outcomes × 2 sides × levels and nothing else — takers are
// IOC. No steering, no cancel-pool heuristic: the number of resting orders is a parameter, because on a real
// venue it is the makers' ladders. That matters under `L3_COMMIT=bookhash`, where every epoch boundary hashes
// the whole resting set and an unbounded workload ends up measuring the commitment instead of the venue.
//
// Pure, deterministic and offline: no chain, no signing, no clock of its own (`now` is injected). It emits op
// INTENTS — `{ t, kind, outcome, buy, price, size, userIndex, ioc, postOnly }` — and the caller signs them, which
// is the one thing a generator must not pretend to have done.
//
//   const f = createFlow({ market: '19085', fair: [0.62, 0.38], makers: 4, levels: 2, boost: 2, seed: 7 });
//   const op = f.next();                       // one intent, or null if there is nothing to do this instant
//   f.acked(op.ref, hash);                     // the hash the signer produced, so the quote can be cancelled
//   f.sync(state.bookFor(market, 0).orders()); // reconcile against the real book, once a second
//   f.stat();                                  // spread, depth, fair deviation, requotes, fills, inventory
import { quoteMath, skewOf, sizeForNotional, needsRequote, quoteMode } from '../quote-math.js';

const num = (v, d) => { const n = Number(v); return Number.isFinite(n) ? n : d; };
const clamp = (x, lo, hi) => Math.min(hi, Math.max(lo, x));
const r6 = (x) => Number(Number(x).toFixed(6));
const sum = (a) => a.reduce((x, y) => x + y, 0);
/// the matcher keeps price and size as 18-decimal bigints; a caller (or a test) may hand over plain units. One
/// helper, so a wei value is never averaged together with a unit value and reported as a spread.
const scale = (v) => { const n = Number(v); if (!Number.isFinite(n)) return 0; return n > 1e9 ? n / 1e18 : n; };

/// xorshift32: a seeded stream good enough for a workload and cheap enough to call a million times a second.
/// Deterministic on purpose — a load run that cannot be replayed cannot be bisected.
export function rng(seed = 1) {
  let s = (seed >>> 0) || 0x9e3779b9;
  return () => { s ^= s << 13; s >>>= 0; s ^= s >>> 17; s ^= s << 5; s >>>= 0; return s / 4294967296; };
}
/// a standard normal from two uniforms (Box–Muller). The fair's step is gaussian because a price's is.
export function gauss(rnd) {
  const u = Math.max(1e-12, rnd()), v = rnd();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}
/// probabilities → logits, and back through softmax, so a random walk cannot leave the simplex however long it
/// runs. A walk on the probabilities themselves has to be clamped at 0 and 1, and a clamp is a wall the price
/// piles up against.
///
/// It is log(p), NOT the binary log-odds log(p/(1−p)). softmax is the inverse of log(p) up to a constant and the
/// two agree only for a two-outcome market, by symmetry; used as a general inverse, log-odds quietly re-weights
/// the distribution — it took a fair of 62c and set the book up at 72.7c, which looked exactly like the first few
/// seconds of the random walk.
export const toLogits = (ps) => ps.map((p) => Math.log(clamp(num(p, 0.5), 1e-6, 1 - 1e-6)));
export function softmax(zs) {
  const m = Math.max(...zs), es = zs.map((z) => Math.exp(z - m)), t = sum(es);
  return es.map((e) => r6(clamp(e / t, 1e-4, 1 - 1e-4)));
}

/**
 * createFlow({ market, outcomes, fair, confidence, … }) → { next, acked, sync, stat, fairs, setFair, close }
 *
 *   market      the shard's market id (every intent carries it back)
 *   outcomes    how many outcomes the market has; `fair` is one probability each, normalised to sum to 1
 *   confidence  the fair's confidence, as the odds service reports it: under `minConf` the spread widens, under
 *               `quoteMinConf` the maker shows a floor quote instead of a price claim, which is a branch of the
 *               live strategy worth putting under load rather than assuming
 *   endsAt      when the market closes; inside `closeGuardMs` the makers pull everything, as they do live
 *   makers      how many independent maker clients quote this market — each with its own inventory and skew
 *   levels      rungs a side; level 0 is the touch, each further rung one `ladderTick` wider
 *   boost       the one dial for "more volume": scales makers × levels and the taker rate together, because
 *               turning up takers alone just empties the book and turning up makers alone never trades
 *   takers      share of ops that cross the touch;  informed  share of those that see the walk first
 *   volPerSec   the fair's volatility, in logits a second — what makes re-quoting happen at all
 */
export function createFlow({
  market, outcomes = 2, fair = null, confidence = 0.8, bandWidth = 0.04, endsAt = null,
  makers = 2, levels = 2, size = 12, takeSize = 3, tick = 0.01, ladderTick = null,
  halfMin = 0.015, bandK = 0.15, skewMax = 0.01, requoteMs = 45000, maxRelHalf = 0.25,
  minConf = 0.3, quoteMinConf = 0.12, lowConfSpread = 0.1, floorHalf = 0.1, floorSizeFraction = 0.25,
  minNotional = 0.01, closeGuardMs = 60000, boost = 1,
  takers = 0.35, informed = 0.2, volPerSec = 0.02, seed = 1, now = Date.now,
} = {}) {
  const B = Math.max(1, num(boost, 1));
  const M = Math.max(1, Math.round(num(makers, 2) * Math.sqrt(B)));
  const L = Math.max(1, Math.round(num(levels, 2) * Math.sqrt(B)));
  const O = Math.max(1, Math.min(32, Math.round(num(outcomes, 2))));
  const T = clamp(num(takers, 0.35), 0, 0.95);
  const t = Math.max(1e-4, num(tick, 0.01));
  const rung = Math.max(t, num(ladderTick, t));
  const rnd = rng(seed);

  // the fair, as logits, so the walk is unbounded and the probabilities are not
  let z = toLogits(Array.isArray(fair) && fair.length === O ? fair : Array.from({ length: O }, () => 1 / O));
  let ps = softmax(z);
  let lastWalk = null;
  let conf = clamp(num(confidence, 0.8), 0, 1);

  // quotes: `${maker}|${outcome}|${side}|${level}` → { price, size, at, hash, ref, kind }
  const quotes = new Map();
  const byHash = new Map();                     // hash → key, for sync()
  const refs = new Map();                       // ref → key, for acked()
  const pos = Array.from({ length: M }, () => new Array(O).fill(0));
  // the whole book's touch (what a taker crosses) and the touch EXCLUDING our own tracked quotes (what a
  // post-only quote must not cross). They have to be two numbers: a maker handed its own best bid as "somebody
  // else's" pulls one tick inside itself at every re-quote, and the ladder ratchets into a crossed book within a
  // few seconds — the kind of bug a noise generator can never produce because it never re-quotes.
  const best = Array.from({ length: O }, () => ({ bid: null, ask: null, bidSize: 0, askSize: 0 }));
  const others = Array.from({ length: O }, () => ({ bid: null, ask: null }));
  let ref = 0, placed = 0, cancelled = 0, requoted = 0, taken = 0, filled = 0, filledSize = 0, floors = 0, pulled = 0;
  let cursor = 0, pendingCancelKey = null;
  let drift = new Array(O).fill(0);
  // THE GRID WALK IS GATED, and it has to be. When every level is quoted and the fair has not moved, a call that
  // walks the whole grid runs quoteMath M × O × 2 × L times to decide there is nothing to do: at 10k ops/s on an
  // 8-maker × 4-level grid that is over a million quote computations a second inside the LOAD GENERATOR, which
  // would show up as the venue being slower than it is. The generator is a measuring instrument; its own cost is
  // measurement error. So a full pass happens only when something could have changed — the fair moved by half a
  // tick, a fill landed, or the re-quote TTL may have expired — and otherwise next() goes straight to a taker.
  let dirty = true, lastGrid = 0, gridWalks = 0, gridSkips = 0;
  const key = (m, o, side, l) => `${m}|${o}|${side}|${l}`;

  /// walk the fair forward to `at`. Called from next() — the walk is driven by the clock, not by the op count,
  /// so the same run at twice the rate has the same volatility per second and twice the ops between re-quotes.
  function walk(at) {
    // The FIRST call establishes the clock and walks nothing. Seeding lastWalk from the injected `now` at
    // construction looks equivalent and is not: a caller that passes its own `at` — every test, and the soak
    // worker, which drives from its own loop clock — hands over a timestamp that may sit before the one the
    // constructor read, dt comes out negative, and the walk never advances for the whole run. The fair then never
    // moves, nothing is ever re-quoted, and the measurement is of the noise generator this replaced.
    if (lastWalk == null) { lastWalk = num(at, 0); return; }
    const dt = Math.max(0, (num(at, 0) - lastWalk) / 1000);
    if (!(dt > 0)) return;
    lastWalk = num(at, 0);
    const vol = num(volPerSec, 0.02) * Math.sqrt(dt);
    if (!(vol > 0)) return;
    const before = ps.slice();
    z = z.map((v) => v + gauss(rnd) * vol);
    ps = softmax(z);
    drift = ps.map((v, i) => v - before[i]);
    for (let i = 0; i < O; i++) if (Math.abs(drift[i]) >= t / 2) { dirty = true; break; }
  }

  /// the band the maker quotes inside. Live this comes from the estimators' disagreement; here it is the walk's
  /// own width, which is the honest analogue: the spread should widen with the volatility it has to survive.
  const bandOf = (p) => [clamp(p - num(bandWidth, 0.04) / 2, 0, 1), clamp(p + num(bandWidth, 0.04) / 2, 0, 1)];

  /// what maker `m` wants to show on (outcome, side, level) right now, or null if it should show nothing
  function want(m, o, side, l, at) {
    const mode = quoteMode({ fair: { p: ps[o], n: 3, confidence: conf, at, stale: false }, endsAt, now: at, minConf: quoteMinConf, closeGuardMs, floor: true });
    if (mode.mode === 'cancel') return null;
    const floor = mode.mode === 'floor';
    const p = floor ? (mode.p != null ? num(mode.p, 1 / O) : 1 / O) : ps[o];
    const q = quoteMath({
      p, band: floor ? null : bandOf(p),
      halfMin: floor ? Math.max(num(floorHalf, 0.1), halfMin) : halfMin,
      minHalf: halfMin, bandK, tick: t, maxRelHalf,
      skew: skewOf({ position: pos[m][o], size, max: skewMax }),
      confidence: conf, minConf, lowConfSpread,
      others: others[o].bid == null && others[o].ask == null ? null : { bid: others[o].bid, ask: others[o].ask },
    });
    const base = side === 'bid' ? q.bid - l * rung : q.ask + l * rung;
    const price = r6(clamp(base, t, 1 - t));
    if (!(price > 0) || !(price < 1)) return null;
    const sz = sizeForNotional({ size: size * (floor ? num(floorSizeFraction, 0.25) : 1) / Math.max(1, l + 1), price, minNotional });
    if (!(sz > 0)) return null;
    return { price, size: r6(sz), floor };
  }

  /// one taker. It crosses the touch of the side the walk favours (informed) or a coin flip (noise), IOC so the
  /// resting set stays the makers' ladders and nothing else.
  function take(at) {
    const o = Math.floor(rnd() * O);
    const b = best[o];
    if (b.bid == null && b.ask == null) return null;             // nothing to take yet
    const knows = rnd() < clamp(num(informed, 0.2), 0, 1);
    const buy = knows ? drift[o] > 0 : rnd() < 0.5;
    const px = buy ? b.ask : b.bid;
    if (px == null) return null;
    // pay through the touch by a rung, so a taker is not refused by a quote that moved a tick under it
    const price = r6(clamp(buy ? px + rung : px - rung, t, 1 - t));
    const sz = sizeForNotional({ size: Math.max(0.01, num(takeSize, 3) * (0.5 + rnd())), price, minNotional });
    taken += 1;
    return { t: 'add', kind: 'take', ref: `t${++ref}`, market, outcome: o, buy, price, size: r6(sz), userIndex: M + Math.floor(rnd() * 4), ioc: true, postOnly: false };
  }

  /// the next maker action, walking the grid once. Returns null when every quote is where it should be.
  function makerAction(at) {
    if (!dirty && at - lastGrid < Math.max(50, Math.min(1000, num(requoteMs, 45000) / 4))) { gridSkips += 1; return null; }
    gridWalks += 1;
    const total = M * O * 2 * L;
    for (let n = 0; n < total; n++) {
      const i = (cursor + n) % total;
      const l = i % L, side = ((i / L) | 0) % 2 === 0 ? 'bid' : 'ask', o = (((i / L) | 0) / 2 | 0) % O, m = ((i / (L * 2 * O)) | 0) % M;
      const k = key(m, o, side, l);
      const cur = quotes.get(k) || null;
      const w = want(m, o, side, l, at);
      if (!w) {
        if (cur && cur.hash) { cursor = i + 1; pulled += 1; return cancelOf(k, cur); }
        continue;
      }
      if (!needsRequote({ current: cur && cur.hash ? { price: cur.price, at: cur.at } : null, want: w.price, tick: t, now: at, requoteMs })) continue;
      // a quote that is already resting has to be pulled before the replacement goes in: post-only would reject
      // a second order at the same level, and leaving it would double the maker's exposure at the old price.
      // The cursor stays ON this level (not i + 1) so the very next call re-quotes what was just pulled — advance
      // it and the grid walks on, leaving the touch empty until it comes round again, which on a wide grid is
      // long enough to show up as a hole in the book under load.
      if (cur && cur.hash) { cursor = i; requoted += 1; return cancelOf(k, cur); }
      cursor = i + 1;
      placed += 1; if (w.floor) floors += 1;
      const r = `q${++ref}`; refs.set(r, k);
      quotes.set(k, { price: w.price, size: w.size, at, hash: null, ref: r, kind: w.floor ? 'floor' : 'quote', maker: m, outcome: o, side });
      return { t: 'add', kind: w.floor ? 'floor' : 'quote', ref: r, market, outcome: o, buy: side === 'bid', price: w.price, size: w.size, userIndex: m, ioc: false, postOnly: true };
    }
    // a whole pass with nothing to do: the book is where the makers want it until something moves
    dirty = false; lastGrid = at;
    return null;
  }
  function cancelOf(k, cur) {
    cancelled += 1;
    byHash.delete(cur.hash);
    quotes.delete(k);
    pendingCancelKey = k;
    return { t: 'cancel', kind: 'cancel', ref: `c${++ref}`, market, outcome: cur.outcome, hash: cur.hash, userIndex: cur.maker };
  }

  return {
    get makers() { return M; },
    get levels() { return L; },
    get outcomes() { return O; },
    fairs: () => ps.slice(),
    /// set the fair from outside — the odds service's real number for this market, when the harness has one
    setFair(next, c = null) {
      if (Array.isArray(next) && next.length === O) { z = toLogits(next); ps = softmax(z); }
      if (c != null) conf = clamp(num(c, conf), 0, 1);
      dirty = true;
    },
    /**
     * next() → one op intent, or null when the book is fully quoted and there is nothing to take.
     * A cancel is always followed by its replacement on the next call: pendingCancelKey rewinds the cursor so the
     * level that was just pulled is the one re-quoted, instead of the grid moving on and leaving a hole.
     */
    next(at = num(now(), 0)) {
      walk(at);
      if (pendingCancelKey) { pendingCancelKey = null; const a = makerAction(at); if (a) return a; }
      if (rnd() < T) { const a = take(at); if (a) return a; }
      return makerAction(at) || take(at);
    },
    /// the hash the caller's signer produced for an intent. Only quotes need it (a cancel names a hash already).
    acked(r, hash) {
      const k = refs.get(r); refs.delete(r);
      if (!k) return false;
      const q = quotes.get(k);
      if (!q || q.ref !== r) return false;
      q.hash = String(hash); byHash.set(q.hash, k);
      return true;
    },
    /**
     * sync(books) — reconcile against the shard's real books: `books[outcome]` is that outcome's resting list
     * from the matcher, each `{ hash, price, remaining|size, buy }`, in wei or in units. A single flat array is
     * accepted and read as outcome 0.
     *
     * This is where fills enter the model, and they enter from the matcher rather than from a guess. A tracked
     * quote that is no longer resting was filled (or cancelled by us and already dropped); one whose remaining
     * shrank was partly filled. Either way the maker's position moves, and the position is what skews the next
     * quote — so the harness's inventory is the inventory the book actually gave it.
     */
    sync(books = []) {
      const rows = Array.isArray(books[0]) || books.length === 0 ? books : [books];
      const seen = new Map();
      for (let o = 0; o < O; o++) { best[o] = { bid: null, ask: null, bidSize: 0, askSize: 0 }; others[o] = { bid: null, ask: null }; }
      for (let o = 0; o < rows.length && o < O; o++) {
        const b = best[o], x = others[o];
        for (const r of rows[o] || []) {
          const h = String(r?.hash || '');
          if (!h) continue;
          const price = scale(r.price), rem = scale(r.remaining !== undefined ? r.remaining : r.size);
          if (!(price > 0) || !(rem > 0)) continue;
          seen.set(h, { rem, buy: !!r.buy, price, outcome: o });
          if (r.buy) {
            if (b.bid == null || price > b.bid) { b.bid = price; b.bidSize = rem; } else if (price === b.bid) b.bidSize += rem;
            if (!byHash.has(h) && (x.bid == null || price > x.bid)) x.bid = price;
          } else {
            if (b.ask == null || price < b.ask) { b.ask = price; b.askSize = rem; } else if (price === b.ask) b.askSize += rem;
            if (!byHash.has(h) && (x.ask == null || price < x.ask)) x.ask = price;
          }
        }
      }
      for (const [h, k] of [...byHash]) {
        const q = quotes.get(k);
        if (!q) { byHash.delete(h); continue; }
        const live = seen.get(h);
        const gone = !live;
        const got = gone ? q.size : Math.max(0, q.size - live.rem);
        if (got > 1e-9) {
          filled += 1; filledSize += got;
          pos[q.maker][q.outcome] += (q.side === 'bid' ? got : -got);
          dirty = true;                                  // a fill frees a level and moves the skew: re-quote
        }
        if (gone) { quotes.delete(k); byHash.delete(h); dirty = true; } else if (got > 1e-9) q.size = live.rem;
      }
      return { tracked: byHash.size, resting: seen.size };
    },
    /**
     * stat() — what a load run should report about the BOOK, not only about the pipeline.
     *
     * Throughput and finality say whether the venue kept up. They say nothing about whether what it kept up with
     * was a market. These do: the touch spread in cents, the depth at the touch, and how far the mid sits from
     * the fair the makers are quoting around — the three numbers that separate a liquid book from a fast one.
     */
    stat() {
      const spreads = [], devs = [], depths = [];
      for (let o = 0; o < O; o++) {
        const b = best[o];
        if (b.bid != null && b.ask != null && b.ask > b.bid) {
          spreads.push((b.ask - b.bid) * 100);
          devs.push(Math.abs((b.ask + b.bid) / 2 - ps[o]) * 100);
        }
        depths.push(b.bidSize + b.askSize);
      }
      const mean = (a) => (a.length ? r6(sum(a) / a.length) : null);
      const inv = pos.map((p) => r6(sum(p.map(Math.abs))));
      return {
        makers: M, levels: L, outcomes: O, grid: M * O * 2 * L,
        placed, cancelled, requoted, pulled, floors, taken, fills: filled, filledSize: r6(filledSize),
        quotes: quotes.size, tracked: byHash.size, gridWalks, gridSkips,
        spreadCents: mean(spreads), fairDevCents: mean(devs), depthAtTouch: mean(depths),
        fair: ps.map((p) => r6(p)), confidence: r6(conf), inventory: inv, inventoryMax: inv.length ? Math.max(...inv) : 0,
      };
    },
    close() { quotes.clear(); byHash.clear(); refs.clear(); },
  };
}

/**
 * flowsFor({ markets, mids, ... }) → one flow per shard, which is one per market.
 *
 * `mids` is what scripts/market-snapshot.mjs prints: the odds service's fair price in cents for each open market,
 * in the same order as the shard list. A soak run on markets all priced at 50c is a different experiment from one
 * on the venue's real prices, and the difference shows up in the commitment cost, so the default is reported
 * rather than hidden.
 */
export function flowsFor({ markets = [], mids = [], outcomes = 2, seed = 1, ...rest } = {}) {
  return markets.map((m, i) => {
    const cents = num(mids[i % Math.max(1, mids.length)], NaN);
    const p = Number.isFinite(cents) && cents > 0 && cents < 100 ? cents / 100 : 1 / Math.max(1, outcomes);
    const fair = outcomes === 2 ? [p, r6(1 - p)] : Array.from({ length: outcomes }, (_, k) => (k === 0 ? p : r6((1 - p) / (outcomes - 1))));
    return createFlow({ ...rest, market: String(m), outcomes, fair, seed: seed + i * 7919 });
  });
}
