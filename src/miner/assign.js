// engine/l3/miner/assign.js — which miner mines which shard, so that adding a miner adds THROUGHPUT.
//
// Today the fleet does not divide the work at all: every miner runs every shard the engine lists, so the
// replication factor IS the miner count. Three miners means every order is verified three times; a fourth miner
// means four times. Verification is the whole cost of finality (§6), so the venue's capacity is
//
//     (total miner cores x per-core verify rate) / replication factor
//
// and when replication tracks the fleet size, that denominator grows exactly as fast as the numerator. Adding
// miners buys redundancy nobody asked for and not one extra order a second. Measured on Fly: three machines at
// ~5,400 orders/s, and the only way the growth test got to 16,131 was by giving each new machine NEW shards.
//
// Hold replication FIXED at R and divide the shards instead, and the denominator stops moving: 29 shards over 9
// miners at R=3 is ~10 shards each rather than 29, which is a third of the work per machine.
//
// CHOOSING R, which is where the first version of this reasoning was wrong. "Replication is work without
// capacity" is only half true, and the half it misses is measured: an A/B on RunPod, 6 miner pods and 12 markets,
// threshold 2, same machines and same workload, differing only in R —
//
//   R=6 (every miner every shard, 6 verifications an order)   11,896 orders/s   backlog 7-20, FLAT    p50 ~190 ms
//   R=3 (shards divided, 3 verifications an order)              2,451 orders/s   backlog 1,185→6,703   p50 ~570 ms
//
// Halving the work per miner made it WORSE, because finality is an order statistic: a batch is final when the
// T-th fastest of its R voters has agreed, so for a fixed threshold T, a larger R means a slow miner does not
// gate the batch. With a per-voter "prompt" probability of 0.8, T=2 of R=6 clears promptly 99.8 % of the time and
// T=2 of R=3 only 89.6 %. On machines that are oversubscribed or merely uneven — which is every rented machine —
// R=3 against T=2 leaves one straggler of slack, and that cost more than the halved verification bought.
// (Partly confounded: arm B's books were twice as deep, 18,000 resting against 9,000, and under L3_COMMIT=bookhash
// every epoch boundary hashes the resting set — one more reason that switch matters.)
//
// So the rule is not "minimise R". It is: divide the shards AND keep R comfortably above the threshold, R >= T+2
// where the fleet is big enough to afford it. Per-miner load is R·S/N, which still falls as N grows, while the
// order statistic stays strong. Six miners could not afford R=3 with T=2; twelve can afford R=4 or 5 and get both.
//
// The assignment is rendezvous hashing (highest random weight): score every miner for a shard with
// keccak256(shard ‖ miner) and take the top R. That gives four properties this needs and a coordinator cannot:
//
//   deterministic   every miner computes the same answer from the same (shards, miners, R) — no agreement
//                   protocol, no leader, no epoch. "In sync" is a property of the inputs, not of a negotiation.
//   order-free      the answer does not depend on the order the roster happens to arrive in.
//   balanced        each miner gets ~R·S/N shards, and no more than `slack` over it. Plain rendezvous is not
//                   good enough here: measured, the heaviest miner came out 23-56 % above ideal, and because a
//                   shard reaches finality at the speed of the threshold-th fastest miner covering it, the
//                   heaviest miner IS the latency of every shard it touches. So placement is capacity-capped —
//                   a shard skips a miner already at its cap and goes to the next best — which is consistent
//                   hashing with bounded loads. Coverage always wins over balance: if R miners cannot be found
//                   under the cap, the best-weighted are used anyway and the shard is still fully covered.
//   minimal churn   a miner joining or leaving moves only the shards whose top-R membership actually changed —
//                   about R·S/N of them — and never reshuffles the rest. Rebalancing a live fleet is affordable.
//
// SAFETY, which matters more than balance. Two rules:
//
//   1. N <= R means every miner runs every shard. With three miners and R=3 that is EXACTLY today's behaviour,
//      so this can ship switched on and changes nothing until the fleet is bigger than its replication factor.
//   2. `keep` — a miner does not DROP a shard it is already mining unless it can see R other miners covering it.
//      A roster that is stale or partial on one miner would otherwise let several of them abandon the same shard
//      at once, and a shard with fewer than `threshold` live miners silently stops reaching finality. Holding on
//      is cheap; dropping early is a liveness failure nobody gets an alert for.
import { keccak256 } from './merkle.js';

const lower = (a) => String(a || '').toLowerCase();
const enc = new TextEncoder();

/// the weight of (shard, miner): the first 8 bytes of keccak256("<shard>|<miner>") as an unsigned integer. 64
/// bits is far more than enough to make ties vanishingly unlikely, and the tie-break below makes them harmless.
function weight(shard, miner) {
  const h = keccak256(enc.encode(`${shard}|${lower(miner)}`));
  let w = 0n;
  for (let i = 0; i < 8; i++) w = (w << 8n) | BigInt(h[i]);
  return w;
}

/**
 * coverageOf(shard, miners, replicas) → the miners that should mine this shard, best weight first.
 * Fewer than `replicas` only when the roster is smaller than that.
 */
export function coverageOf(shard, miners, replicas = 3) {
  const R = Math.max(1, Number(replicas) || 1);
  const roster = [...new Set((miners || []).map(lower))].filter(Boolean);
  if (!roster.length) return [];
  if (roster.length <= R) return roster.slice().sort();          // everyone, deterministically ordered
  return ranked(shard, roster).slice(0, R);
}

/// every miner for this shard, best weight first. Ties break on the address so the order is the same everywhere.
function ranked(shard, roster) {
  return roster
    .map((m) => ({ m, w: weight(shard, m) }))
    .sort((a, b) => (a.w < b.w ? 1 : a.w > b.w ? -1 : a.m < b.m ? -1 : 1))
    .map((x) => x.m);
}

/**
 * assign({ shards, miners, replicas }) → { byShard, byMiner, under, replicas, everyone }
 *
 *   byShard    Map<shard, miner[]>
 *   byMiner    Map<miner, shard[]>   (sorted, so it is comparable between runs)
 *   under      shards covered by fewer than `replicas` miners — i.e. the roster is too small
 *   everyone   true when N <= R and therefore every miner has every shard (today's fleet)
 */
export function assign({ shards, miners, replicas = 3, slack = 0.1 } = {}) {
  const R = Math.max(1, Number(replicas) || 1);
  const list = [...new Set((shards || []).map(String))].filter(Boolean).sort();
  const roster = [...new Set((miners || []).map(lower))].filter(Boolean).sort();
  const byShard = new Map(), byMiner = new Map(), under = [];
  for (const m of roster) byMiner.set(m, []);
  const everyone = roster.length > 0 && roster.length <= R;
  // the cap: the ideal load plus `slack`, never below the ideal (or nothing would fit) and never below 1
  const ideal = roster.length ? (R * list.length) / roster.length : 0;
  const cap = everyone ? Infinity : Math.max(1, Math.ceil(ideal * (1 + Math.max(0, Number(slack) || 0))));
  const load = new Map(roster.map((m) => [m, 0]));
  for (const s of list) {
    let who;
    if (!roster.length) who = [];
    else if (everyone) who = roster.slice();
    else {
      const order = ranked(s, roster);
      who = order.filter((m) => load.get(m) < cap).slice(0, R);
      // coverage beats balance: top up from the best-weighted even if they are at their cap
      if (who.length < R) for (const m of order) { if (who.length >= R) break; if (!who.includes(m)) who.push(m); }
    }
    byShard.set(s, who);
    if (who.length < R) under.push(s);
    for (const m of who) { byMiner.get(m).push(s); load.set(m, load.get(m) + 1); }
  }
  for (const m of roster) byMiner.get(m).sort();
  return { byShard, byMiner, under, replicas: R, everyone, cap: Number.isFinite(cap) ? cap : null, ideal };
}

/**
 * shardsFor(me, { shards, miners, replicas, running }) → { mine, added, dropped, held, everyone, under }
 *
 * What THIS miner should run, including the `keep` rule: a shard in `running` that the assignment no longer gives
 * us is only dropped when R OTHER miners are assigned to it. Anything else is held, and reported as `held`, so a
 * fleet mid-rebalance over-covers rather than under-covers.
 */
export function shardsFor(me, { shards, miners, replicas = 3, running = [], slack = 0.1 } = {}) {
  const self = lower(me);
  const a = assign({ shards, miners, replicas, slack });
  const want = new Set(a.byMiner.get(self) || []);
  const now = new Set((running || []).map(String));
  const held = [];
  for (const s of now) {
    if (want.has(s)) continue;
    const who = (a.byShard.get(s) || []).filter((m) => m !== self);
    // not ours any more — but only let go once enough others have it
    if (who.length >= a.replicas) continue;
    held.push(s); want.add(s);
  }
  const mine = [...want].sort();
  return {
    mine,
    added: mine.filter((s) => !now.has(s)),
    dropped: [...now].filter((s) => !want.has(s)).sort(),
    held: held.sort(),
    everyone: a.everyone,
    under: a.under,
  };
}

/// how much an assignment moves when the roster changes — the number that decides whether rebalancing a live
/// fleet is affordable. `churn(before, after)` counts (miner, shard) pairs that are not in both.
export function churn(before, after) {
  let moved = 0;
  const keys = new Set([...before.byMiner.keys(), ...after.byMiner.keys()]);
  for (const m of keys) {
    const b = new Set(before.byMiner.get(m) || []), a = new Set(after.byMiner.get(m) || []);
    for (const s of a) if (!b.has(s)) moved++;
    for (const s of b) if (!a.has(s)) moved++;
  }
  return moved;
}

// ---------------------------------------------------------------- how many replicas the fleet can afford
/**
 * replicasFor({ miners, shards, ordersPerSec, perMinerCapacity, threshold, utilisation })
 *   → { replicas, perMinerShards, perMinerLoad, budget, need, why }
 *
 * R should not be a constant, and measuring it twice is what showed why. Two A/Bs on RunPod, same machines and
 * workload, differing only in R:
 *
 *     6 miners, 12 markets, T=2    R=6  → 11,896 orders/s, backlog flat     R=3 → 2,451/s, backlog 1,185→6,703
 *    12 miners, 24 markets, T=2    R=12 →  4,183 orders/s, backlog flat      R=4 → 3,100/s, queue GREW (0.74x)
 *
 * Dividing the shards lost both times, which is the opposite of the arithmetic about verification cost — and the
 * arithmetic is not wrong, it is about a regime those runs were nowhere near:
 *
 *   LATENCY-BOUND (both runs, ~4,000 orders/s). Each miner was verifying ~4,000/s against a capacity of ~108,000/s
 *   (8 vCPU x 13,500). Verification was 4 % of the machine; nothing was waiting on it. What finality waited on was
 *   the T-th fastest of R voters, and that gets FASTER as R grows: with T=2, two of twelve answer sooner than two
 *   of four. Cutting R to save work the miners were not short of simply made every batch wait longer, the backlog
 *   grew, and the rate controller backed off. More replicas is more speed here.
 *
 *   CAPACITY-BOUND. When the venue's order rate approaches ONE miner's verification capacity, "every miner runs
 *   every shard" is a wall that no number of miners moves, because every miner must verify the whole venue. At
 *   that point dividing the shards is not an optimisation, it is the only way through — and R should be the
 *   smallest value that still tolerates stragglers.
 *
 * So R is chosen, per poll, as the largest the fleet can afford without any miner exceeding a verification budget:
 *
 *     load(R) = (R·S/N) x r        each miner's shards x the orders/s on a shard
 *     R       = clamp( floor(budget·N / (S·r)), threshold+2, N )
 *
 * At a low rate that is R = N, which is today's behaviour and the one that measured best. As the rate rises R
 * falls out of the formula on its own, and when even R = threshold+2 will not fit, `need` says how many miners
 * the fleet would have to have for it to fit — which is the signal to add machines rather than a silent overload.
 */
export function replicasFor({ miners = 0, shards = 0, ordersPerSec = 0, perMinerCapacity = 108000, threshold = 2, utilisation = 0.6 } = {}) {
  const N = Math.max(0, Math.floor(Number(miners) || 0));
  const S = Math.max(0, Math.floor(Number(shards) || 0));
  const T = Math.max(1, Math.floor(Number(threshold) || 1));
  const floorR = Math.min(Math.max(T + 2, 1), Math.max(N, 1));            // straggler tolerance, never above the roster
  if (!N || !S) return { replicas: Math.min(floorR, Math.max(N, 1)), perMinerShards: 0, perMinerLoad: 0, budget: 0, need: 0, why: 'no roster or no shards' };
  const budget = Math.max(1, Number(perMinerCapacity) || 1) * Math.min(1, Math.max(0.05, Number(utilisation) || 0.6));
  const rate = Math.max(0, Number(ordersPerSec) || 0);
  const perShard = S > 0 ? rate / S : 0;
  // the whole venue fits inside one miner: keep every replica, because latency is what is binding and more voters
  // answer sooner
  if (perShard <= 0 || rate <= budget) {
    const R = N;
    return { replicas: R, perMinerShards: S, perMinerLoad: rate, budget, need: 0, why: rate <= budget ? 'latency-bound: the venue fits in one miner, so keep every replica' : 'no measured rate' };
  }
  const affordable = Math.floor((budget * N) / (S * perShard));           // = budget·N / rate
  const R = Math.max(floorR, Math.min(N, affordable));
  const perMinerShards = Math.ceil((R * S) / N);
  const perMinerLoad = perMinerShards * perShard;
  // if even the floor does not fit, say how many miners would make it fit at the floor
  const need = perMinerLoad > budget ? Math.ceil((floorR * rate) / budget) : 0;
  return {
    replicas: R, perMinerShards, perMinerLoad, budget, need,
    why: R >= N ? 'the whole venue still fits in one miner'
      : R > floorR ? 'capacity-bound: R trimmed so every miner stays inside its verification budget'
        : need ? `capacity-bound: even R=${floorR} does not fit — the fleet needs ~${need} miner(s)`
          : `capacity-bound: at the straggler floor R=${floorR}`,
  };
}
