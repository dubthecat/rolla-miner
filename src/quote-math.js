// lib/quote-math.js — the market maker's quote, as arithmetic.
//
// This is the whole of what decides where a maker's two prices go: the half-spread from the fair's own band, its
// floor, the cap relative to the price, the inventory skew, the tick rounding, the clamps, the post-only pull
// inside somebody else's resting order, and the three decisions above them — requote, floor, or pull. No wallet,
// no chain, no viem: given a fair it returns two numbers.
//
// It lives here, on its own, for one reason. `engine/mm/strategies/l3-quoter.js` is the live maker and
// `engine/l3/miner/flow.js` is the load harness that is supposed to put the venue's OWN books under load. If the
// harness approximated the maker's spread the measurement would be of the approximation: a book quoted 3c wide
// around a static mid is not the book the venue has, and every number measured on it — the epoch commitment's
// cost, the fill rate, the adverse selection — belongs to a venue that does not exist. Both sides import this, so
// a change to the spread rule changes the simulation in the same commit.
//
// Everything here is pure and total: same inputs, same outputs, no throw on a missing or nonsense field. The
// comments are the reasoning behind numbers that were each paid for by a live book behaving badly; they are kept
// verbatim from the strategy they came out of.

const num = (v, d) => { const n = Number(v); return Number.isFinite(n) ? n : d; };
const clamp = (x, lo, hi) => Math.min(hi, Math.max(lo, x));
const r6 = (x) => Number(Number(x).toFixed(6));

// ---- pure ------------------------------------------------------------------------------------------------------------
/**
 * sizeForNotional({ size, price, minNotional, tick }) → size
 *
 * RollaBook refuses an order whose notional (price × size) is under 0.01 (engine/l3/book.js), so a small quote on a
 * cheap outcome is rejected outright: the maintainer's floor size of 1.25 at a price of 0.005 is 0.00625 and bounced
 * with "order too small". The size is raised until the notional clears the minimum with a little room, never lowered.
 */
export function sizeForNotional({ size = 5, price = 0.5, minNotional = 0.01 } = {}) {
  const p = num(price, 0); const s = Math.max(0, num(size, 0)); const m = Math.max(0, num(minNotional, 0.01));
  if (!(p > 0) || !(m > 0) || s * p >= m) return r6(s);
  return r6(Math.max(s, Math.ceil((m * 1.05 / p) * 100) / 100));
}
/// the inventory skew in price: long (position > 0) → both quotes lower, short → higher; ±max at one quote size of inventory
export function skewOf({ position = 0, size = 5, max = 0.01 } = {}) { const s = size > 0 ? size : 1; return r6(-clamp(num(position, 0) / s, -1, 1) * Math.max(0, num(max, 0))); }
/**
 * quoteMath({ p, band, halfMin, minHalf, bandK, skew, tick, others, confidence, minConf, lowConfSpread }) → { bid, ask, half, lowConf }
 *   p       the fair probability of the outcome;  band  [lo, hi] of the fair
 *   half-spread = max(halfMin, (hi − lo) / 2 × bandK, and — under the strategy's confidence floor minConf — (1 − confidence)
 *   × lowConfSpread, a spread that widens as the fair loses confidence); bid = p − half + skew rounded down to the tick,
 *   ask = p + half + skew rounded up; clamped inside (0, 1); never crossing another user's best (others.bid / others.ask):
 *   post-only would reject
 *   `minHalf` is the narrowest half-spread that still PAYS this maker, and it is the only floor under the relative cap
 *   below; `halfMin` is the spread the caller wants. They differ for a floor quote, which wants a wide spread but must
 *   not keep it when the price has no room for it.
 */
export function quoteMath({ p, band = null, halfMin = 0.015, minHalf = null, bandK = 0.5, skew = 0, tick = 0.005, others = null, confidence = null, minConf = 0.3, lowConfSpread = 0.1, maxRelHalf = 0.35 } = {}) {
  const fair = clamp(num(p, 0.5), 0, 1); const t = Math.max(1e-4, num(tick, 0.005));
  const bandHalf = Array.isArray(band) && band.length === 2 ? Math.max(0, (num(band[1], fair) - num(band[0], fair)) / 2) : 0;
  const lowConf = confidence != null && Number.isFinite(Number(confidence)) && Number(confidence) < num(minConf, 0.3);
  const lowTerm = lowConf ? (1 - clamp(num(confidence, 0), 0, 1)) * Math.max(0, num(lowConfSpread, 0)) : 0;
  let half = Math.max(Math.max(0, num(halfMin, 0)), bandHalf * Math.max(0, num(bandK, 0)), lowTerm);
  // A spread only means something relative to the price. The same 10c half-spread is an ordinary market at 50c and an
  // absurd one at 4c, where it drives the bid through zero and the clamp below pins it to the bottom tick: 20 of 143
  // resting bids sat at 0.005 on 2026-11-07, next to cards reading 33%. It is equally wrong at the top, where it
  // produced asks at 99.5c on a fair of 99.2c — nobody pays 100c for a 99c outcome. So the half-spread is capped at a
  // fraction of the room between the fair and the nearer bound, and floored at halfMin so the quote still pays.
  // The floor under that cap is what it takes to PAY, not what the caller asked for. Those were the same number, so a
  // floor quote — halfMin 0.10, the widest the strategy posts — had a cap of max(0.10, 0.35 × room), which is 0.10 for
  // every price under 29c. The cap could not bite exactly where it was written to: on a fair of 5c the bid went to the
  // bottom tick, and the live maker was posting `3 @ 0.010` next to a card reading 5%. With minHalf at the ordinary
  // 0.015 the same quote becomes 3c/7c — still the widest book on the venue at that price, and a real one.
  const room = Math.min(fair, 1 - fair);
  const pays = Math.max(0, num(minHalf != null ? minHalf : halfMin, 0));
  const relCap = Math.max(pays, Math.max(0, num(maxRelHalf, 0.35)) * room);
  if (half > relCap) half = relCap;
  const down = (x) => r6(Math.floor(x / t + 1e-9) * t), up = (x) => r6(Math.ceil(x / t - 1e-9) * t);
  let bid = down(fair - half + skew), ask = up(fair + half + skew);
  bid = r6(clamp(bid, t, 1 - 2 * t)); ask = r6(clamp(ask, bid + t, 1 - t));
  if (others) {
    if (others.ask != null && bid >= num(others.ask, 1) - 1e-9) bid = down(num(others.ask, 1) - t);
    if (others.bid != null && ask <= num(others.bid, 0) + 1e-9) ask = up(num(others.bid, 0) + t);
    bid = r6(clamp(bid, t, 1 - 2 * t)); ask = r6(clamp(ask, bid + t, 1 - t));
  }
  return { bid: r6(bid), ask: r6(ask), half: r6(half), skew: r6(skew), lowConf };
}
/// re-quote when the wanted price moved by a tick or more, or the resting quote is older than requoteMs (or there is none)
export function needsRequote({ current = null, want, tick = 0.005, now = Date.now(), requoteMs = 20000 } = {}) {
  if (!current || current.price == null) return true;
  if (Math.abs(num(want, 0) - num(current.price, 0)) >= num(tick, 0.005) - 1e-9) return true;
  return now - num(current.at, 0) >= requoteMs;
}
/// why every quote of a market must be pulled now (null = keep quoting). `minConf` here is the QUOTE floor (MM_QUOTE_MIN_CONF):
/// a stale fair, no estimate (a uniform fair at confidence 0) or a confidence under it means no quote at all
export function shouldCancel({ fair = null, endsAt = null, now = Date.now(), minConf = 0.3, closeGuardMs = 60000 } = {}) {
  if (!fair) return 'no fair';
  if (fair.stale) return 'fair stale';
  if (!(fair.n > 0)) return 'no estimate';
  if (num(fair.confidence, 0) < minConf) return `confidence ${num(fair.confidence, 0).toFixed(2)} < ${minConf}`;
  if (endsAt != null && num(endsAt, 0) - now <= closeGuardMs) return 'market closing';
  return null;
}
/**
 * quoteMode({ fair, endsAt, now, minConf, closeGuardMs, floor, floorMaxAgeMs }) → { mode, why, p }
 *
 * The maintainer's decision for one market: 'quote' on a usable fair, 'floor' when there is no usable view but the
 * market should still show a book, 'cancel' when quoting would be wrong whatever the price.
 *
 * Why a floor exists at all: every open market is supposed to be tradeable, and a market the estimators cannot price
 * is exactly the one a person is most likely to have an opinion about. Refusing to quote it left 40 of 236 books
 * empty and the rest one-sided. A floor quote is an honest statement of ignorance — the widest spread the strategy
 * will show and a fraction of the size — not a price claim. It centres on the fair only while that fair is recent;
 * past `floorMaxAgeMs` a stale estimate is worse than none, so it centres on 1/n (0.5 on a binary) instead.
 *
 * 'cancel' is reserved for the market closing: a quote that cannot be pulled before expiry is a free option for
 * whoever is watching. A missing runtime or a missing outcome token is handled by the caller, not here, because
 * neither is about price.
 */
export function quoteMode({ fair = null, endsAt = null, now = Date.now(), minConf = 0.3, closeGuardMs = 60000, floor = true, floorMaxAgeMs = 600000 } = {}) {
  if (endsAt != null && num(endsAt, 0) - now <= closeGuardMs) return { mode: 'cancel', why: 'market closing', p: null };
  const why = shouldCancel({ fair, endsAt, now, minConf, closeGuardMs });
  if (!why) return { mode: 'quote', why: null, p: null };
  if (!floor) return { mode: 'cancel', why, p: null };
  // centre on the fair only while it is fresh enough to mean anything
  const fresh = !!fair && fair.n > 0 && num(fair.at, 0) > 0 && now - num(fair.at, 0) <= Math.max(0, num(floorMaxAgeMs, 600000));
  return { mode: 'floor', why, p: fresh ? fair.p : null };
}

