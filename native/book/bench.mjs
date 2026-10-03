// native/book/bench.mjs — the Node side of the side-by-side: the SAME mixed workload as bench.cpp, through the
// JavaScript reference book (engine/l3/matcher.js). Same LCG, same draw order, same swap-removed live list,
// same tick grid (prices are tick × 1e14 bigints), same one-sample-every-1,000-ops latency method.
//   node bench.mjs mixed <ops> [seed] [span] [step]
//   node bench.mjs patho <depth>
//   node bench.mjs rtt <ops> [batch] [fsync 0|1]
//                                  the round-trip cost of the process boundary: <ops> orders through
//                                  engine/l3/native.js in groups of <batch> (1 = one order per round trip),
//                                  with the journal fsynced per ack or not
//   node bench.mjs sock <ops> [batch] [fsync 0|1]
//                                  the same, but asynchronously over bookd's Unix socket with the replies
//                                  read as they arrive — the shape a sequencer would use, and the only one
//                                  where the process boundary is cheaper than matching in JavaScript
//   node bench.mjs timer
import { createBook } from '../../src/matcher.js';
import { createNativeBook } from '../../src/native.js';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';

const TICK = 10n ** 14n, E = 10n ** 18n;
const ns = () => process.hrtime.bigint();
const pct = (v, p) => (v.length ? v[Math.round(p * (v.length - 1))] : 0);
const hwm = () => { try { return Number(/VmHWM:\s+(\d+)/.exec(fs.readFileSync('/proc/self/status', 'utf8'))[1]) / 1024; } catch { return process.memoryUsage().rss / 1048576; } };
function lat(what, v) {
  v.sort((a, b) => a - b);
  console.log(`  ${what} latency (n=${v.length}): p50 ${(pct(v, 0.5) / 1000).toFixed(2)} µs · p99 ${(pct(v, 0.99) / 1000).toFixed(2)} µs · p99.9 ${(pct(v, 0.999) / 1000).toFixed(2)} µs · max ${((v[v.length - 1] || 0) / 1000).toFixed(2)} µs`);
}

function mixed(ops, seed, span, step) {
  const book = createBook();
  let s = seed >>> 0; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
  const live = []; const samples = []; let mid = 5000, nfills = 0, ncancels = 0, nhit = 0;
  const seg = Math.max(1, Math.floor(ops / 10)); const rates = [];
  const t0 = ns(); let tseg = t0;
  for (let i = 0; i < ops; i++) {
    const sample = i % 1000 === 0;
    if (live.length && rnd() < 0.1) {
      const k = Math.floor(rnd() * live.length); const id = live[k]; live[k] = live[live.length - 1]; live.pop();
      const a = sample ? ns() : 0n;
      const hit = book.cancel(id);
      if (sample) samples.push(Number(ns() - a));
      ncancels++; if (hit) nhit++;
    } else {
      const buy = rnd() < 0.5, cross = rnd() < 0.3;
      const off = Math.floor(rnd() * span) * step;
      let tick = buy ? (cross ? mid + off : mid - 1 - off) : (cross ? mid - off : mid + 1 + off);
      tick = tick < 1 ? 1 : tick > 9999 ? 9999 : tick;
      const user = 'u' + (1 + Math.floor(rnd() * 4096));
      const size = BigInt(1 + Math.floor(rnd() * 100)) * E;
      const ioc = rnd() < 0.1;
      mid += rnd() < 0.5 ? -1 : 1;
      mid = mid < 2500 ? 2500 : mid > 7500 ? 7500 : mid;
      const ord = { hash: 'o' + (i + 1), user, buy, price: BigInt(tick) * TICK, size, ioc };
      const a = sample ? ns() : 0n;
      const r = book.add(ord);
      if (sample) samples.push(Number(ns() - a));
      nfills += r.fills.length;
      if (r.rested) live.push(ord.hash);
    }
    if ((i + 1) % seg === 0) { const now = ns(); rates.push(seg / (Number(now - tseg) / 1e9)); tseg = now; }
  }
  const dt = Number(ns() - t0) / 1e9;
  console.log(`mixed ${ops} ops in ${dt.toFixed(3)} s → ${Math.round(ops / dt)} ops/s (span ${span} × step ${step})`);
  console.log(`  ${nfills} fills · ${ncancels} cancels (${nhit} hit) · ${book.size} resting · seq ${book.seq}`);
  lat('per-op', samples);
  console.log(`  peak RSS ${hwm().toFixed(1)} MB`);
  console.log(`  ops/s per 10% segment: ${rates.map((r) => Math.round(r)).join(' ')}`);
}

function patho(depth) {
  { // sweep
    const book = createBook();
    const t0 = ns();
    for (let i = 0; i < depth; i++) book.add({ hash: 'o' + (i + 1), user: 'u' + (i % 4096), buy: false, price: 5000n * TICK, size: E });
    const tb = Number(ns() - t0) / 1e9;
    const t1 = ns();
    const r = book.add({ hash: 'taker', user: 'sweeper', buy: true, price: 5000n * TICK, size: BigInt(depth) * E });
    const ts = Number(ns() - t1) / 1e9;
    console.log(`patho sweep depth ${depth}: build ${tb.toFixed(3)} s (${Math.round(depth / tb)} adds/s) · sweep ${r.fills.length} fills in ${ts.toFixed(3)} s → ${Math.round(r.fills.length / ts)} fills/s (${Math.round(ts * 1e9 / r.fills.length)} ns/fill)`);
    console.log(`  book empty after the sweep: ${book.size === 0 ? 'yes' : 'NO'} · remaining ${r.remaining}`);
  }
  { // cancel-all in random order
    const book = createBook();
    for (let i = 0; i < depth; i++) book.add({ hash: 'o' + (i + 1), user: 'u' + (i % 4096), buy: false, price: 5000n * TICK, size: E });
    const ids = Array.from({ length: depth }, (_, i) => 'o' + (i + 1));
    let s = 12345; const rnd = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
    for (let i = depth - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); [ids[i], ids[j]] = [ids[j], ids[i]]; }
    const samples = []; const t0 = ns();
    for (let i = 0; i < depth; i++) { const a = ns(); book.cancel(ids[i]); samples.push(Number(ns() - a)); }
    const dt = Number(ns() - t0) / 1e9;
    console.log(`patho cancel-all depth ${depth}: ${dt.toFixed(3)} s → ${Math.round(depth / dt)} cancels/s · book ${book.size}`);
    lat('cancel', samples);
    console.log(`  peak RSS ${hwm().toFixed(1)} MB`);
  }
}

/// the cost of the process boundary, which is the whole question for L3_NATIVE: the match itself is ~0.3 µs
function rtt(ops, batch, fsync) {
  const book = createNativeBook({ fsync });
  try {
    const mk = (i) => ({ hash: 'r' + i, user: 'u' + (i % 512), buy: i % 2 === 0, price: BigInt(4000 + (i % 2000)) * TICK, size: E });
    book.add(mk(0));                                            // warm the pipe
    const samples = []; let fills = 0;
    const t0 = ns();
    for (let i = 1; i <= ops; i += batch) {
      const group = [];
      for (let k = 0; k < batch && i + k <= ops; k++) group.push(mk(i + k));
      const a = ns();
      const rs = batch === 1 ? [book.add(group[0])] : book.addMany(group);
      samples.push(Number(ns() - a) / group.length);
      for (const r of rs) fills += r.fills.length;
    }
    const dt = Number(ns() - t0) / 1e9;
    console.log(`rtt ${ops} orders · batch ${batch} · fsync ${fsync ? 'on' : 'off'} → ${Math.round(ops / dt)} orders/s · ${(dt / ops * 1e6).toFixed(1)} µs per order · ${fills} fills`);
    lat('per-order (batch amortised)', samples);
    const st = book.stat();
    console.log(`  bookd: resting ${st.resting} · seq ${st.seq} · journal ${st.journal} records · transcript ${st.stateHash.toString(16)}`);
  } finally { book.close(); }
}

/// asynchronous, pipelined, over the Unix socket: no synchronous round trip, replies counted as they land
async function sock(ops, batch, fsync) {
  const bin = path.join(path.dirname(new URL(import.meta.url).pathname), 'bookd');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rollbook-sock-'));
  const sk = path.join(dir, 'b.sock');
  const d = spawn(bin, ['--listen', sk, '--journal', path.join(dir, 'b.journal'), '--fsync', fsync ? '1' : '0', '--quiet'], { stdio: ['ignore', 'ignore', 'inherit'] });
  try {
    for (let i = 0; i < 100 && !fs.existsSync(sk); i++) await new Promise((r) => setTimeout(r, 20));
    const c = net.connect(sk); await new Promise((r, j) => { c.once('connect', r); c.once('error', j); });
    const frame = (id, user, buy, tick, size) => {
      const b = Buffer.alloc(51); b.writeUInt32LE(47, 0); b[4] = 1;
      b.writeBigUInt64LE(BigInt(id), 5); b.writeBigUInt64LE(BigInt(user), 13);
      b[21] = buy ? 1 : 0; b[22] = 0; b.writeUInt32LE(0, 23); b.writeBigInt64LE(BigInt(tick), 27);
      b.writeBigUInt64LE(size & 0xffffffffffffffffn, 35); b.writeBigUInt64LE(size >> 64n, 43); return b;
    };
    let buf = Buffer.alloc(0), replies = 0;
    c.on('data', (x) => {
      buf = buf.length ? Buffer.concat([buf, x]) : x;
      let i = 0;
      while (i + 4 <= buf.length) { const len = buf.readUInt32LE(i); if (i + 4 + len > buf.length) break; replies++; i += 4 + len; }
      buf = buf.subarray(i);
    });
    const t0 = ns();
    for (let i = 1; i <= ops; i += batch) {
      const parts = [];
      for (let k = 0; k < batch && i + k <= ops; k++) parts.push(frame(i + k, ((i + k) % 512) + 1, (i + k) % 2 === 0, 4000 + ((i + k) % 2000), E));
      if (!c.write(Buffer.concat(parts))) await new Promise((r) => c.once('drain', r));
      const want = Math.min(i + batch - 1, ops);
      while (replies < want) await new Promise((r) => setImmediate(r));
    }
    const dt = Number(ns() - t0) / 1e9;
    console.log(`sock ${ops} orders · batch ${batch} · fsync ${fsync ? 'on' : 'off'} → ${Math.round(ops / dt)} orders/s · ${(dt / ops * 1e6).toFixed(2)} µs per order · ${replies} replies`);
    c.end();
  } finally { d.kill(); fs.rmSync(dir, { recursive: true, force: true }); }
}

const [what = 'mixed', n = '1000000', seed = '7', span = '2000', step = '1'] = process.argv.slice(2);
if (what === 'mixed') mixed(Number(n), Number(seed), Number(span), Number(step));
else if (what === 'patho') patho(Number(n));
else if (what === 'rtt') rtt(Number(n), Math.max(1, Number(seed)), String(span) !== '0');
else if (what === 'sock') await sock(Number(n), Math.max(1, Number(seed)), String(span) !== '0');
else if (what === 'timer') { const v = []; for (let i = 0; i < 100000; i++) { const a = ns(); v.push(Number(ns() - a)); } v.sort((a, b) => a - b); console.log(`hrtime.bigint() pair cost: p50 ${pct(v, 0.5)} ns · p99 ${pct(v, 0.99)} ns`); }
else console.error('usage: node bench.mjs [mixed <ops> [seed] [span] [step] | patho <depth> | rtt|sock <ops> [batch] [fsync] | timer]');
