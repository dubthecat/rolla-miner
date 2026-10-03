// native/book/diff.mjs — the differential check: the same deterministic workload through the JavaScript
// reference book (engine/l3/matcher.js) and through the native book (./trace), compared line for line.
//
// matcher.js is the specification. If these two disagree, the C++ is wrong. The workload generator below is a
// verbatim copy of `run(seed, count)` from engine/l3/matcher.test.mjs — including which rnd() draws happen in
// which order, because the stream of random numbers is itself part of the fixture — and trace.cpp is a port of
// this same function. The summary line also compares the resting count, the sequence number and the transcript
// hash (book_state_hash), which is recomputed here in BigInt: that the hash matches across two independent
// implementations is what lets L3 miners vote on a batch by its hash.
//
//   node diff.mjs [seed] [count] ...        (default: seeds 7 and 11 at 200000 ops, the hard requirement)
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createBook } from '../../src/matcher.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const E = 10n ** 18n, TICK = 10n ** 14n;
const M64 = (1n << 64n) - 1n;
const mix = (h, x) => {                                    // the exact hash_mix() of book.hpp, in BigInt
  h = (h ^ (x & M64)) & M64;
  h = (h * 0x100000001b3n) & M64;
  h = h ^ (h >> 29n);
  h = (h * 0xbf58476d1ce4e5b9n) & M64;
  return h & M64;
};

/// a verbatim copy of matcher.test.mjs's run(), with the transcript hash and the fill lines collected
function run(seed, count) {
  const book = createBook();
  let s = seed >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
  const users = ['u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'u7', 'u8']; const live = []; const out = [];
  let h = 0xcbf29ce484222325n;
  const uid = (u) => BigInt(users.indexOf(u) + 1);
  for (let i = 0; i < count; i++) {
    if (live.length && rnd() < 0.1) {
      const hh = live.splice(Math.floor(rnd() * live.length), 1)[0]; const c = book.cancel(hh);
      out.push(c ? `c${hh}` : `x${hh}`);
      if (c) h = mix(mix(mix(h, 3n), BigInt(book.seq)), BigInt(hh.slice(1)) + 1n);
      continue;
    }
    const buy = rnd() < 0.5; const cross = rnd() < 0.3; const mid = 50; const off = Math.floor(rnd() * 20);
    const price = buy ? (cross ? mid + off : mid - 1 - off) : (cross ? mid - off : mid + 1 + off);
    const ord = { hash: `o${i}`, user: users[Math.floor(rnd() * users.length)], buy, price: BigInt(price) * E / 100n, size: BigInt(1 + Math.floor(rnd() * 100)) * E, ioc: rnd() < 0.1 };
    const seq0 = book.seq;                                   // the taker's own sequence number is seq0 + 1
    const r = book.add(ord); if (r.rested) live.push(ord.hash);
    const id = BigInt(i) + 1n, tick = ord.price / TICK;
    h = mix(mix(mix(mix(mix(mix(h, 1n), BigInt(seq0 + 1)), id), uid(ord.user)), tick),
            ((ord.size >> 64n) ^ (ord.size & M64)) ^ (BigInt(ord.ioc ? 2 : 0) << 32n) ^ (ord.buy ? 1n : 0n));
    for (const f of r.fills) {
      out.push(`${f.makerHash}>${f.takerHash}@${f.price}:${f.size}`);
      h = mix(mix(mix(mix(mix(h, 2n), BigInt(f.seq)), BigInt(f.makerHash.slice(1)) + 1n), BigInt(f.takerHash.slice(1)) + 1n),
              ((f.size >> 64n) ^ (f.size & M64)) ^ (f.price / TICK));
    }
  }
  return { out, resting: book.size, seq: book.seq, thash: h };
}

function native(seed, count) {
  const bin = path.join(here, 'trace');
  const r = spawnSync(bin, [String(seed), String(count), '--summary'], { maxBuffer: 1 << 30, encoding: 'utf8' });
  if (r.error || r.status !== 0) { console.error(`trace failed (${r.error?.message || r.status}) — run make first`); process.exit(2); }
  const lines = r.stdout.split('\n');
  while (lines.length && lines[lines.length - 1] === '') lines.pop();
  const sum = lines.pop() || '';
  const kv = Object.fromEntries(sum.replace(/^#\s*/, '').split(/\s+/).map((p) => p.split('=')));
  return { out: lines, resting: Number(kv.resting), seq: BigInt(kv.seq || 0), thash: BigInt('0x' + (kv.thash || '0')) };
}

const argv = process.argv.slice(2);
const cases = argv.length ? [[Number(argv[0]), Number(argv[1] || 200000)]] : [[7, 200000], [11, 200000]];
let bad = 0;
for (const [seed, count] of cases) {
  const t0 = Date.now(); const a = run(seed, count); const tjs = Date.now() - t0;
  const t1 = Date.now(); const b = native(seed, count); const tcc = Date.now() - t1;
  let first = -1;
  const n = Math.min(a.out.length, b.out.length);
  for (let i = 0; i < n; i++) if (a.out[i] !== b.out[i]) { first = i; break; }
  const same = first < 0 && a.out.length === b.out.length;
  const seqOk = BigInt(a.seq) === b.seq, restOk = a.resting === b.resting, hashOk = a.thash === b.thash;
  console.log(`seed ${seed} · ${count.toLocaleString()} ops · js ${a.out.length.toLocaleString()} lines in ${tjs} ms · native ${b.out.length.toLocaleString()} lines in ${tcc} ms`);
  console.log(`  lines ${same ? 'IDENTICAL' : 'DIFFER'} · resting ${a.resting}/${b.resting} ${restOk ? 'ok' : 'DIFFER'} · seq ${a.seq}/${b.seq} ${seqOk ? 'ok' : 'DIFFER'} · transcript hash ${a.thash.toString(16)}/${b.thash.toString(16)} ${hashOk ? 'ok' : 'DIFFER'}`);
  if (!same) {
    bad++;
    if (first >= 0) {
      console.log(`  first divergence at line ${first}:`);
      for (let i = Math.max(0, first - 3); i <= Math.min(n - 1, first + 3); i++) console.log(`    ${i === first ? '>>' : '  '} js     ${a.out[i]}\n    ${i === first ? '>>' : '  '} native ${b.out[i]}`);
    } else {
      console.log(`  same prefix, different length: js ${a.out.length} vs native ${b.out.length}; js tail ${JSON.stringify(a.out.slice(n, n + 3))} native tail ${JSON.stringify(b.out.slice(n, n + 3))}`);
    }
  }
  if (!seqOk || !restOk || !hashOk) bad++;
}
console.log(bad ? `FAIL (${bad})` : 'PASS — fill for fill, same sequence numbers, same transcript hash');
process.exit(bad ? 1 : 0);
