// engine/l3/matcher.js — a pure, deterministic price-time limit order book for ONE (market, outcome).
//
// No I/O, no clock, no chain: the same sequence of add/cancel calls always produces the same fills, which is
// what lets the journal be replayed on boot and lets a validator re-run a shard's day and compare. Prices and
// sizes are bigints (1e18-scaled collateral per share, 1e18 outcome-token units), exactly what RollaBook settles.
//
//   add(order)     → { fills, rested, remaining, reason, seq }   a taker crosses the far side FIFO at the MAKER's price;
//                                                            what is left rests (unless ioc); post-only never crosses.
//                                                            `seq` is the order's own sequence number (0 if it was
//                                                            never numbered: duplicate, bad order); every fill
//                                                            carries `makerRemaining`, what is left of the maker
//                                                            after it (0n → the maker left the book). Both feed the
//                                                            incremental state commitment (miner/commit-state.js)
//                                                            and change nothing about matching.
//   cancel(hash)   → the resting order, or null
//   depth(n)       → the top n levels per side
//
// Self-trades are allowed (the owner's decision): an order matches the same user's resting orders like anyone
// else's, strict price-time. On chain the settlement nets per (user, token), so a self-fill is a zero move.
//
// Structures: a Map price → level per side (bigint keys compare by value), a sorted array of prices per side
// (bids descending, asks ascending, binary-search insert/remove), an intrusive doubly-linked FIFO inside a level
// (O(1) pop at the head on a fill, O(1) unlink on a cancel — the array version was quadratic in level depth:
// 12.6 s to sweep a 100k-deep level), hash → order for cancels. The native book (native/book) is the same
// semantics in C++ and agrees with this one fill for fill (native/book/diff.mjs).

export function createBook() {
  const bids = new Map(), asks = new Map();   // price → { price, head, tail, total, count }
  const bidPrices = [], askPrices = [];        // bids desc, asks asc
  const byHash = new Map();                    // hash → resting order node
  let seq = 0;

  const lower = (arr, p, desc) => { let lo = 0, hi = arr.length; while (lo < hi) { const mid = (lo + hi) >> 1; if (desc ? arr[mid] > p : arr[mid] < p) lo = mid + 1; else hi = mid; } return lo; };
  const insertPrice = (arr, p, desc) => { const i = lower(arr, p, desc); if (arr[i] !== p) arr.splice(i, 0, p); };
  const removePrice = (arr, p, desc) => { const i = lower(arr, p, desc); if (arr[i] === p) arr.splice(i, 1); };

  function rest(o) {
    const side = o.buy ? bids : asks, prices = o.buy ? bidPrices : askPrices;
    let lvl = side.get(o.price);
    if (!lvl) { lvl = { price: o.price, head: null, tail: null, total: 0n, count: 0 }; side.set(o.price, lvl); insertPrice(prices, o.price, o.buy); }
    o.prev = lvl.tail; o.next = null; if (lvl.tail) lvl.tail.next = o; else lvl.head = o; lvl.tail = o;
    lvl.total += o.remaining; lvl.count++; o.level = lvl; byHash.set(o.hash, o);
  }
  function unlink(lvl, o) {
    if (o.prev) o.prev.next = o.next; else lvl.head = o.next;
    if (o.next) o.next.prev = o.prev; else lvl.tail = o.prev;
    lvl.count--; o.level = null; o.prev = o.next = null;
  }
  function dropIfEmpty(lvl, buy) { if (lvl.count === 0) { (buy ? bids : asks).delete(lvl.price); removePrice(buy ? bidPrices : askPrices, lvl.price, buy); return true; } return false; }
  const pub = (o) => ({ hash: o.hash, user: o.user, buy: o.buy, price: o.price, size: o.size, remaining: o.remaining, postOnly: o.postOnly, ioc: o.ioc, ts: o.ts, seq: o.seq });

  /// o: { hash, user, buy, price (bigint), size (bigint), remaining? (bigint), postOnly?, ioc?, ts? }
  function add(o) {
    if (byHash.has(o.hash)) return { fills: [], rested: false, remaining: byHash.get(o.hash).remaining, reason: 'duplicate', seq: 0 };
    if (!(o.price > 0n) || !(o.size > 0n)) return { fills: [], rested: false, remaining: 0n, reason: 'bad order', seq: 0 };
    const order = { hash: o.hash, user: o.user, buy: !!o.buy, price: o.price, size: o.size, remaining: o.remaining ?? o.size, postOnly: !!o.postOnly, ioc: !!o.ioc, ts: o.ts || 0, seq: ++seq, prev: null, next: null, level: null };
    const far = order.buy ? asks : bids, farPrices = order.buy ? askPrices : bidPrices;
    const crosses = (p) => (order.buy ? p <= order.price : p >= order.price);
    if (order.postOnly && farPrices.length && crosses(farPrices[0])) return { fills: [], rested: false, remaining: order.remaining, reason: 'would cross', seq: order.seq };
    const fills = [];
    while (farPrices.length && order.remaining > 0n && crosses(farPrices[0])) {
      const lvl = far.get(farPrices[0]);
      let r = lvl.head;
      while (r && order.remaining > 0n) {
        const size = r.remaining < order.remaining ? r.remaining : order.remaining;
        r.remaining -= size; order.remaining -= size; lvl.total -= size;
        fills.push({ seq: ++seq, makerHash: r.hash, takerHash: order.hash, maker: r.user, taker: order.user, price: r.price, size, takerBuys: order.buy, makerRemaining: r.remaining });
        if (r.remaining === 0n) { const nx = r.next; unlink(lvl, r); byHash.delete(r.hash); r = nx; }
      }
      if (!dropIfEmpty(lvl, !order.buy)) break;   // the level still has size: the taker is done
    }
    let rested = false;
    if (order.remaining > 0n && !order.ioc) { rest(order); rested = true; }
    return { fills, rested, remaining: order.remaining, reason: null, seq: order.seq };
  }

  function cancel(hash) {
    const o = byHash.get(hash); if (!o) return null;
    const lvl = o.level;
    if (lvl) { unlink(lvl, o); lvl.total -= o.remaining; dropIfEmpty(lvl, o.buy); }
    byHash.delete(hash); seq++;
    return pub(o);
  }

  const levels = (side, prices, n) => prices.slice(0, n).map((p) => { const l = side.get(p); return { price: p, size: l.total, orders: l.count }; });
  function depth(n = 25) { return { bids: levels(bids, bidPrices, n), asks: levels(asks, askPrices, n), seq }; }
  function best() { return { bid: bidPrices.length ? bidPrices[0] : null, ask: askPrices.length ? askPrices[0] : null }; }
  function get(hash) { const o = byHash.get(hash); return o ? pub(o) : null; }
  function orders(user = null) { const out = []; for (const o of byHash.values()) if (!user || o.user === user) out.push(pub(o)); return out.sort((a, b) => a.seq - b.seq); }
  /// what a taker would get right now, without touching the book (for quotes)
  function preview(buy, price, size) {
    const far = buy ? asks : bids, farPrices = buy ? askPrices : bidPrices; let left = size, cost = 0n, n = 0;
    for (const p of farPrices) { if (buy ? p > price : p < price) break; for (let r = far.get(p).head; r && left > 0n; r = r.next) { const s = r.remaining < left ? r.remaining : left; left -= s; cost += s * p; n++; } if (left === 0n) break; }
    return { filled: size - left, cost, fills: n, avg: size - left > 0n ? cost / (size - left) : null };
  }
  return { add, cancel, depth, best, get, orders, preview, get size() { return byHash.size; }, get seq() { return seq; } };
}
