// scripts/market-snapshot.mjs — the live venue's open markets and what they are actually worth, as a workload.
//
//   node scripts/market-snapshot.mjs                      # against https://rollmarkets.com
//   node scripts/market-snapshot.mjs --url http://… --max 40 --env
//
// Why: a soak whose every market is priced at 50c produces books that look nothing like the venue's. Half of them
// sit nowhere near their own fair, the resting set spreads evenly instead of clustering at the touch, and the epoch
// commitment then hashes a shape the real venue never has. This reads the open set and the odds service's fair
// price for each, and prints the two lists sim-soak.mjs wants:
//
//   SOAK_SHARD_LIST=19085,21249,…       the real market ids, one shard each
//   SOAK_MIDS=52,89,7,…                 that market's fair price in cents, same order
//
// The odds service is admin-only, so the token is read from ~/.rollacoasta-keys/rollmarkets-admin.token or
// ADMIN_TOKEN. Without it the fair prices are unavailable and every market falls back to 50 — which is reported
// rather than hidden, because a soak run on 50s is a different experiment and should not be mistaken for this one.
import fs from 'node:fs';
import os from 'node:os';

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > 0 && process.argv[i + 1] ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes(f);
const URL_ = String(arg('--url', 'https://rollmarkets.com')).replace(/\/+$/, '');
const MAX = Number(arg('--max', '0'));
const TOKEN = process.env.ADMIN_TOKEN || (() => { try { return fs.readFileSync(`${os.homedir()}/.rollacoasta-keys/rollmarkets-admin.token`, 'utf8').trim(); } catch { return ''; } })();

const j = async (u, h = {}) => {
  const r = await fetch(URL_ + u, { headers: h, signal: AbortSignal.timeout(45000) });
  if (!r.ok) throw new Error(`${u} → HTTP ${r.status}`);
  return r.json();
};

const board = await j('/v1/predict/onchain');
const now = Math.floor(Date.now() / 1000);
let open = (board.markets || []).filter((m) => !m.resolvedAt && Number(m.endsAt) > now);

let fair = new Map(), why = 'no admin token: every market falls back to 50c';
if (TOKEN) {
  try {
    const odds = await j('/v1/odds', { 'x-admin-token': TOKEN });
    for (const m of (odds.markets || [])) if (Array.isArray(m.p) && m.p.length) fair.set(String(m.marketId), { p: m.p, conf: Number(m.confidence) || 0 });
    why = `${fair.size} market(s) priced by the odds service`;
  } catch (e) { why = `odds unavailable (${e.message})`; }
}

// a market with a fair price first, because that is the point; then the rest, which quote at 50
open.sort((a, b) => (fair.has(String(b.id)) ? 1 : 0) - (fair.has(String(a.id)) ? 1 : 0));
if (MAX > 0) open = open.slice(0, MAX);

const shards = [], mids = [], rows = [];
for (const m of open) {
  const f = fair.get(String(m.id));
  // outcome 0's probability is the YES side, which is the side the simulated book is centred on
  const p = f && Number.isFinite(Number(f.p[0])) ? Number(f.p[0]) : 0.5;
  const cents = Math.max(2, Math.min(98, Math.round(p * 100)));
  shards.push(String(m.id)); mids.push(String(cents));
  rows.push({ id: m.id, cents, conf: f ? f.conf : null, q: String(m.question || '').slice(0, 44) });
}

if (has('--env')) {
  console.log(`SOAK_SHARD_LIST=${shards.join(',')}`);
  console.log(`SOAK_MIDS=${mids.join(',')}`);
} else {
  console.log(`${URL_}: ${open.length} open market(s) · ${why}`);
  for (const r of rows) console.log(`  ${String(r.id).padStart(6)}  ${String(r.cents).padStart(2)}c  conf ${r.conf == null ? ' —  ' : r.conf.toFixed(2)}  ${r.q}`);
  console.log(`\nSOAK_SHARD_LIST=${shards.join(',')}`);
  console.log(`SOAK_MIDS=${mids.join(',')}`);
}
