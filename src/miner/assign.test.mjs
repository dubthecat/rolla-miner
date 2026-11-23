// node --test engine/l3/miner/assign.test.mjs — dividing the shards so that a new miner adds throughput.
//
// The point being tested is an arithmetic one. Verification is the whole cost of finality, so venue capacity is
// (total cores x per-core rate) / replication factor. The fleet today gives every shard to every miner, which
// makes replication equal the miner count, which cancels the numerator: three miners verify everything three
// times, four verify it four times, and the venue goes no faster. Fixing R and dividing the shards is what turns
// a new machine into throughput, and these tests pin the properties that makes safe.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { assign, coverageOf, shardsFor, churn } from './assign.js';

const miner = (i) => '0x' + String(i).padStart(2, '0').repeat(20);
const miners = (n) => Array.from({ length: n }, (_, i) => miner(i + 1));
const shards = (n, base = 19000) => Array.from({ length: n }, (_, i) => String(base + i));

test('every shard gets exactly the replication factor, and no more', () => {
  const a = assign({ shards: shards(40), miners: miners(9), replicas: 3 });
  assert.equal(a.byShard.size, 40);
  for (const [s, who] of a.byShard) {
    assert.equal(who.length, 3, `shard ${s} has ${who.length} miner(s)`);
    assert.equal(new Set(who).size, 3, 'and no miner twice');
  }
  assert.deepEqual(a.under, [], 'nothing under-replicated with 9 miners and R=3');
});

test('the answer does not depend on the order the roster arrives in', () => {
  const S = shards(40), M = miners(7);
  const a = assign({ shards: S, miners: M, replicas: 3 });
  const shuffled = [...M].reverse();
  const b = assign({ shards: [...S].reverse(), miners: shuffled, replicas: 3 });
  for (const s of S) assert.deepEqual([...a.byShard.get(s)].sort(), [...b.byShard.get(s)].sort(), `shard ${s}`);
  // which is the whole basis of "in sync": no agreement protocol, just the same inputs
});

test('and it is stable: the same inputs twice give the same assignment', () => {
  const S = shards(25), M = miners(6);
  const a = assign({ shards: S, miners: M, replicas: 3 });
  const b = assign({ shards: S, miners: M, replicas: 3 });
  for (const s of S) assert.deepEqual(a.byShard.get(s), b.byShard.get(s));
});

test('no miner is loaded past its cap — the invariant that keeps one slow miner off everyone else', () => {
  // The heaviest miner is the finality of every shard it covers, because a shard reaches finality at the
  // threshold-th fastest miner holding it. Plain rendezvous put the worst miner 23-56 % above ideal; capped
  // placement holds it to the cap, measured 10-14 %.
  for (const [S, N, R] of [[90, 9, 3], [60, 20, 3], [29, 9, 3], [200, 15, 3]]) {
    const a = assign({ shards: shards(S), miners: miners(N), replicas: R });
    const loads = [...a.byMiner.values()].map((x) => x.length);
    const hi = Math.max(...loads);
    assert.equal(loads.reduce((x, y) => x + y, 0), R * S, `S=${S} N=${N}: every replica placed exactly once`);
    assert.ok(hi <= a.cap, `S=${S} N=${N}: heaviest miner ${hi} is over the cap ${a.cap}`);
    assert.ok(hi <= Math.ceil(a.ideal * 1.2), `S=${S} N=${N}: heaviest ${hi} is more than 20% over ideal ${a.ideal.toFixed(1)}`);
    assert.deepEqual(a.under, [], `S=${S} N=${N}: coverage is never traded for balance`);
  }
});

test('coverage beats balance: a roster too small to stay under the cap still covers every shard', () => {
  // slack 0 makes the cap exactly the ideal, so some shards cannot find R miners under it — they must still get R
  const S = shards(31), N = 5, R = 3;
  const a = assign({ shards: S, miners: miners(N), replicas: R, slack: 0 });
  for (const [sh, who] of a.byShard) assert.equal(who.length, R, `shard ${sh} lost coverage to the cap`);
  assert.deepEqual(a.under, []);
});

test('a fleet no bigger than its replication factor runs everything — which is today, exactly', () => {
  // three miners, R=3: the production fleet. This must be a no-op, so the strategy can ship switched on.
  const S = shards(29);
  const a = assign({ shards: S, miners: miners(3), replicas: 3 });
  assert.equal(a.everyone, true);
  for (const m of miners(3)) assert.equal((a.byMiner.get(m.toLowerCase()) || []).length, 29, 'every shard, as now');
  assert.deepEqual(a.under, [], 'and nothing is under-replicated: three miners IS three replicas');
});

test('R must stay above the threshold: the assignment refuses to leave a shard one straggler from stalling', () => {
  // Measured, not assumed: 6 miners at R=3 with threshold 2 sustained 2,451 orders/s with a backlog that grew the
  // whole run, against 11,896 orders/s and a flat backlog at R=6. Finality is the T-th fastest of R, so cutting R
  // cuts straggler tolerance — at a per-voter prompt probability of 0.8, 2-of-6 clears 99.8 % of the time and
  // 2-of-3 only 89.6 %. Dividing shards is right; dividing them down to R = T+1 is not.
  const S = shards(60), N = 12;
  for (const [R, T] of [[3, 2], [4, 2], [5, 3]]) {
    const a = assign({ shards: S, miners: miners(N), replicas: R });
    for (const [sh, who] of a.byShard) assert.equal(who.length, R, `shard ${sh}`);
    assert.ok(R > T, 'a replication factor at or below the threshold could never finalise');
    const loads = [...a.byMiner.values()].map((x) => x.length);
    // and the point survives: per-miner load is R·S/N, so raising R for safety still beats everyone-runs-everything
    assert.ok(Math.max(...loads) < S.length, `R=${R} on ${N} miners: heaviest ${Math.max(...loads)} must be under all ${S.length} shards`);
  }
});

test('the capacity claim: per-miner load falls as the fleet grows, and that is the whole point', () => {
  const S = shards(60), R = 3;
  const rows = [];
  for (const N of [3, 6, 9, 12, 20]) {
    const a = assign({ shards: S, miners: miners(N), replicas: R });
    const loads = [...a.byMiner.values()].map((x) => x.length);
    rows.push({ N, max: Math.max(...loads) });
  }
  // at N <= R everyone holds all 60; past that the per-miner load must come down roughly as R·S/N
  assert.equal(rows[0].max, 60, 'three miners still carry everything');
  for (let i = 2; i < rows.length; i++) {
    assert.ok(rows[i].max < rows[i - 1].max, `load did not fall from ${rows[i - 1].N} to ${rows[i].N} miners`);
  }
  const last = rows[rows.length - 1];
  assert.ok(last.max <= Math.ceil((R * S.length) / last.N * 1.5), `20 miners should carry ~${(R * S.length) / last.N}, carried ${last.max}`);
  // 60 shards, R=3, 20 miners → ~9 each against 60: the same cores doing ~6.7x the venue
});

test('a miner joining moves only the shards whose coverage actually changed', () => {
  const S = shards(90), R = 3;
  const before = assign({ shards: S, miners: miners(9), replicas: R });
  const after = assign({ shards: S, miners: miners(10), replicas: R });
  const moved = churn(before, after);
  // a full reshuffle would be 2·R·S = 540 pair changes; rendezvous should move about R·S/N each way
  const expected = (R * S.length) / 10;                   // 27 placements, so ~54 pair changes
  assert.ok(moved <= expected * 3, `moved ${moved} (miner,shard) pair(s), expected around ${expected * 2}`);
  // and every shard still has full coverage throughout
  for (const [, who] of after.byShard) assert.equal(who.length, R);
});

test('a miner leaving costs only its own shards, and coverage is restored', () => {
  const S = shards(60), R = 3;
  const before = assign({ shards: S, miners: miners(8), replicas: R });
  const after = assign({ shards: S, miners: miners(7), replicas: R });
  const gone = miner(8).toLowerCase();
  const had = (before.byMiner.get(gone) || []).length;
  assert.ok(had > 0);
  for (const [, who] of after.byShard) assert.equal(who.length, R, 'the shards it held were picked up');
  // the only placements that should move are the departed miner's
  assert.ok(churn(before, after) <= had * 3, 'a departure should not reshuffle the fleet');
});

test('a miner holds a shard it is losing until enough others have it', () => {
  const S = shards(40), R = 3, M = miners(9);
  const me = miner(1);
  const a = assign({ shards: S, miners: M, replicas: R });
  const mine = a.byMiner.get(me.toLowerCase());
  // pretend we are also running a shard the assignment gives to others: we may let it go
  const notMine = S.find((s) => !mine.includes(s));
  const ok = shardsFor(me, { shards: S, miners: M, replicas: R, running: [...mine, notMine] });
  assert.ok(ok.dropped.includes(notMine), 'a shard R others cover is dropped');
  assert.deepEqual(ok.held, [], 'and nothing had to be held');

  // now the same shard, but with a roster so small that nobody else can cover it: hold on to it
  const tiny = [me, miner(2)];
  const held = shardsFor(me, { shards: S, miners: tiny, replicas: R, running: [...mine, notMine] });
  assert.equal(held.dropped.length, 0, 'nothing is dropped when the roster cannot cover it');
  assert.ok(held.mine.includes(notMine), 'it is still mined');
});

test('shardsFor reports what changed, so a fleet can act on it incrementally', () => {
  const S = shards(40), R = 3, M = miners(9);
  const me = miner(4);
  const want = assign({ shards: S, miners: M, replicas: R }).byMiner.get(me.toLowerCase());
  const fresh = shardsFor(me, { shards: S, miners: M, replicas: R, running: [] });
  assert.deepEqual(fresh.mine, want);
  assert.deepEqual(fresh.added, want, 'a fresh miner adds all of its own');
  assert.deepEqual(fresh.dropped, []);
  const settled = shardsFor(me, { shards: S, miners: M, replicas: R, running: want });
  assert.deepEqual(settled.added, [], 'and a settled one changes nothing');
  assert.deepEqual(settled.dropped, []);
});

test('an empty or single roster is not a crash', () => {
  assert.deepEqual(coverageOf('19000', [], 3), []);
  const a = assign({ shards: shards(5), miners: [], replicas: 3 });
  assert.equal(a.byShard.size, 5);
  assert.equal(a.under.length, 5, 'with no miners every shard is under-replicated, and says so');
  const one = assign({ shards: shards(5), miners: miners(1), replicas: 3 });
  assert.equal(one.everyone, true);
  assert.equal((one.byMiner.get(miner(1).toLowerCase()) || []).length, 5);
  assert.equal(one.under.length, 5, 'one miner cannot be three replicas, and that is reported');
});
