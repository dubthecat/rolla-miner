// native/book/commit_diff.mjs — the differential check for the state commitment: the same random operations
// through the JavaScript tree (engine/l3/miner/commit.js) and the native one (./commit_trace, the C++ of
// commit.hpp), roots compared after every 1,000 operations, and at each checkpoint the native proof of one
// live order compared byte for byte with the JavaScript proof and verified by the JavaScript verifier.
//
// commit.js is the specification (its bytes are merkle.js's). If these two disagree, the C++ is wrong.
//
//   node commit_diff.mjs [seed] [count]        (default: seeds 3 and 5 at 100,000 ops, run in parallel)
import { spawn, spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createCommitTree, verifyCommitProof, encodeCommitProof } from '../../src/miner/commit.js';
import { hex, bytes } from '../../src/miner/merkle.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const SELF = fileURLToPath(import.meta.url);
const word = (v) => { let s = v.toString(16); return s.padStart(64, '0'); };

/// the workload: a mix of inserts (50%), removes (25%) and partial fills (25%) over a live set, with a few
/// deliberate (side, price, seq) collisions, some 256-bit prices and sizes, and a repeated hash now and then
/// (which both sides must refuse). Returns the protocol lines for commit_trace and applies each op to the JS tree.
function workload(seed, count, js) {
  let s = seed >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
  const hexn = (n) => { let h = ''; for (let i = 0; i < n; i++) h += ((rnd() * 256) | 0).toString(16).padStart(2, '0'); return h; };
  const big = () => BigInt('0x' + hexn(32));
  const live = []; const lines = []; const checkpoints = []; let seq = 0, acc = 0;
  for (let i = 1; i <= count; i++) {
    const d = rnd();
    if (live.length && d < 0.25) {
      const k = (rnd() * live.length) | 0; const o = live[k];
      if (rnd() < 0.5) { live[k] = live[live.length - 1]; live.pop(); } else { /* a second remove of the same hash later must be refused */ live.splice(k, 1); }
      lines.push(`r ${o.hash.slice(2)}`); if (js.remove(o.hash)) acc++;
      if (rnd() < 0.05) { lines.push(`r ${o.hash.slice(2)}`); if (js.remove(o.hash)) acc++; }    // already gone
    } else if (live.length && d < 0.5) {
      const k = (rnd() * live.length) | 0; const o = live[k];
      o.remaining = rnd() < 0.1 ? big() : BigInt(1 + ((rnd() * 1e6) | 0)) * 10n ** 12n;
      lines.push(`u ${o.hash.slice(2)} ${word(o.remaining)}`); if (js.setRemaining(o.hash, o.remaining)) acc++;
    } else {
      const collide = live.length && rnd() < 0.05 ? live[(rnd() * live.length) | 0] : null;   // same (side, price, seq), another hash
      const o = collide
        ? { hash: '0x' + hexn(32), user: '0x' + hexn(20), buy: collide.buy, price: collide.price, remaining: BigInt(1 + ((rnd() * 100) | 0)) * 10n ** 18n, seq: collide.seq }
        : { hash: '0x' + hexn(32), user: '0x' + hexn(20), buy: rnd() < 0.5, price: rnd() < 0.05 ? big() : BigInt(1 + ((rnd() * 10000) | 0)) * 10n ** 14n, remaining: rnd() < 0.05 ? big() : BigInt(1 + ((rnd() * 100) | 0)) * 10n ** 18n, seq: ++seq };
      lines.push(`i ${o.hash.slice(2)} ${o.user.slice(2)} ${o.buy ? 1 : 0} ${word(o.price)} ${word(o.remaining)} ${o.seq}`);
      if (js.insert(o)) { acc++; live.push(o); }
      if (rnd() < 0.02) { lines.push(`i ${o.hash.slice(2)} ${o.user.slice(2)} ${o.buy ? 1 : 0} ${word(o.price)} ${word(o.remaining)} ${o.seq}`); if (js.insert(o)) acc++; }   // the same hash again: refused
    }
    if (i % 1000 === 0 || i === count) {
      const probe = live.length ? live[(rnd() * live.length) | 0].hash : null;
      lines.push('?'); if (probe) lines.push(`p ${probe.slice(2)}`);
      checkpoints.push({ op: i, root: js.root(), size: js.size, acc, probe, proof: probe ? js.proof(probe) : null });
    }
  }
  return { lines, checkpoints };
}

function one(seed, count) {
  const js = createCommitTree();
  const t0 = Date.now(); const { lines, checkpoints } = workload(seed, count, js); const tjs = Date.now() - t0;
  const t1 = Date.now();
  const r = spawnSync(path.join(here, 'commit_trace'), [], { input: lines.join('\n') + '\n', maxBuffer: 1 << 30, encoding: 'utf8' });
  const tcc = Date.now() - t1;
  if (r.error || r.status !== 0) { console.error(`commit_trace failed (${r.error?.message || r.status}): ${r.stderr} — run make first`); return 2; }
  const out = r.stdout.split('\n').filter(Boolean);
  let bad = 0, k = 0, proofs = 0;
  for (const c of checkpoints) {
    const m = /^root ([0-9a-f]{64}) size (\d+) acc (\d+)$/.exec(out[k++] || '');
    if (!m) { console.log(`  seed ${seed} op ${c.op}: malformed native line ${JSON.stringify(out[k - 1])}`); bad++; break; }
    const root = '0x' + m[1], size = Number(m[2]), acc = Number(m[3]);
    if (root !== c.root || size !== c.size || acc !== c.acc) {
      if (bad === 0) console.log(`  seed ${seed} op ${c.op}: js root ${c.root} size ${c.size} acc ${c.acc} · native root ${root} size ${size} acc ${acc}`);
      bad++;
    }
    if (c.probe) {
      const pm = /^proof (-|[0-9a-f]+)$/.exec(out[k++] || '');
      if (!pm || pm[1] === '-') { console.log(`  seed ${seed} op ${c.op}: native has no proof of ${c.probe}`); bad++; continue; }
      const native = bytes('0x' + pm[1]), mine = encodeCommitProof(c.proof);
      if (hex(native) !== hex(mine)) { if (bad === 0) console.log(`  seed ${seed} op ${c.op}: proof of ${c.probe} differs (${native.length} vs ${mine.length} bytes)`); bad++; }
      if (!verifyCommitProof(c.proof, c.root)) { console.log(`  seed ${seed} op ${c.op}: the js proof does not verify`); bad++; }
      proofs++;
    }
  }
  const last = checkpoints[checkpoints.length - 1];
  console.log(`seed ${seed} · ${count.toLocaleString()} ops · ${checkpoints.length} checkpoints, ${proofs} proofs · js ${tjs} ms (${(tjs * 1000 / count).toFixed(0)} µs/op, ${js.keccaks.toLocaleString()} keccaks) · native ${tcc} ms incl. spawn · final size ${last.size} root ${last.root.slice(0, 18)}…`);
  console.log(`  roots ${bad ? 'DIFFER' : 'IDENTICAL'} at every checkpoint, sizes and accepted counts ${bad ? 'checked' : 'equal'}, proofs ${bad ? 'checked' : 'byte-identical and verified'}`);
  return bad ? 1 : 0;
}

const argv = process.argv.slice(2);
if (argv[0] === '--one') process.exit(one(Number(argv[1]), Number(argv[2])));
if (argv.length) process.exit(one(Number(argv[0]), Number(argv[1] || 100000)));
// the default: two seeds, each in its own process, so the JavaScript side runs in parallel
const cases = [[3, 100000], [5, 100000]];
const results = await Promise.all(cases.map(([seed, count]) => new Promise((resolve) => {
  const p = spawn(process.execPath, [SELF, '--one', String(seed), String(count)], { stdio: ['ignore', 'pipe', 'inherit'] });
  let out = ''; p.stdout.on('data', (d) => { out += d; });
  p.on('close', (code) => resolve({ seed, code, out }));
})));
let bad = 0;
for (const r of results) { process.stdout.write(r.out); if (r.code) bad++; }
console.log(bad ? `FAIL (${bad})` : 'PASS — same root after every 1,000 ops, same proofs, on both seeds');
process.exit(bad ? 1 : 0);
