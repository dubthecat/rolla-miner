// native/book/commit_bench.mjs — the JavaScript commit tree (engine/l3/miner/commit.js) under the workload of
// commit_bench.cpp, scaled down: n inserts, n/2 partial fills, n/2 removes, µs per operation for each phase.
//   node commit_bench.mjs [n=100000]
import { createCommitTree } from '../../src/miner/commit.js';
import { keccak256, NATIVE_KECCAK } from '../../src/miner/merkle.js';

const n = Number(process.argv[2] || 100000);
let s = 0x5eed; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
const hexn = (k) => { let h = '0x'; for (let i = 0; i < k; i++) h += ((rnd() * 256) | 0).toString(16).padStart(2, '0'); return h; };
const now = () => Number(process.hrtime.bigint()) / 1000;

{ const b = new Uint8Array(97); const t0 = now(); for (let i = 0; i < 20000; i++) b[0] = keccak256(b)[0]; console.log(`keccak256(97 bytes)      ${((now() - t0) / 20000).toFixed(2)} µs   (${NATIVE_KECCAK ? 'the keccak binding' : 'noble, pure JS'})`); }

const t = createCommitTree(); const hashes = [];
let t0 = now();
for (let i = 0; i < n; i++) {
  const o = { hash: hexn(32), user: hexn(20), buy: rnd() < 0.5, price: BigInt(1 + ((rnd() * 10000) | 0)) * 10n ** 14n, remaining: BigInt(1 + ((rnd() * 100) | 0)) * 10n ** 18n, seq: i + 1 };
  t.insert(o); hashes.push(o.hash);
}
let dt = now() - t0; let k0 = t.keccaks;
console.log(`insert       ${String(n).padStart(9)}   ${(dt / n).toFixed(2)} µs/op   ${(k0 / n).toFixed(1)} keccaks/op   size ${t.size}`);
const half = n >> 1; const live = hashes.slice();
t0 = now(); k0 = t.keccaks;
for (let i = 0; i < half; i++) t.setRemaining(live[(rnd() * live.length) | 0], BigInt(1 + ((rnd() * 1e6) | 0)) * 10n ** 12n);
dt = now() - t0;
console.log(`setRemaining ${String(half).padStart(9)}   ${(dt / half).toFixed(2)} µs/op   ${((t.keccaks - k0) / half).toFixed(1)} keccaks/op`);
t0 = now(); k0 = t.keccaks;
for (let i = 0; i < half; i++) { const k = (rnd() * live.length) | 0; t.remove(live[k]); live[k] = live[live.length - 1]; live.pop(); }
dt = now() - t0;
console.log(`remove       ${String(half).padStart(9)}   ${(dt / half).toFixed(2)} µs/op   ${((t.keccaks - k0) / half).toFixed(1)} keccaks/op   size ${t.size}`);
t0 = now(); let r; for (let i = 0; i < 100000; i++) r = t.root(); console.log(`root()                   ${((now() - t0) / 100000).toFixed(3)} µs`);
t0 = now(); let p; for (let i = 0; i < 10000; i++) p = t.proof(live[(rnd() * live.length) | 0]); console.log(`proof()                  ${((now() - t0) / 10000).toFixed(2)} µs   depth ${p.path.length}`);
console.log(`root ${r}   size ${t.size}   keccaks ${t.keccaks.toLocaleString()}`);
