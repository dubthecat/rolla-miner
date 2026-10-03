// engine/l3/native.js — the native book (native/book/bookd) behind the same surface as createBook() from
// matcher.js, so engine/l3/book.js can run either one. Off by default; L3_NATIVE=1 turns it on.
//
// One bookd process per (market, outcome), exactly as L3-SIDECHAIN.md §3 wants it: its own journal, its own
// sequence numbers, its own transcript hash. The engine talks to it over a pair of FIFOs with length-prefixed
// binary frames, synchronously, because book.js matches inside a synchronous call and a promise there would
// change the engine's ordering guarantees. Synchronous means one round trip per order (~25 µs on the bench
// machine, measured in L3-NATIVE-BOOK.md) — far more than the ~0.3 µs the match itself costs, so the win is
// in addMany(), which pipelines a whole arrival batch into one write and one fsync. That is the shape the
// sequencer already has (§3 seals a 50–100 ms batch), and the number to beat is in the doc.
//
// Identity: the native book speaks 64-bit order and user ids, so this module owns the maps between them and
// the engine's hashes and addresses. An id is never reused for a different hash, and the maps are pruned when
// an order leaves the book (filled out, cancelled) — which is why the ADD reply carries the maker's remaining.
//
// Prices: the native book is a tick grid. A price that is not a multiple of tickSize (default 1e14 = 0.0001)
// is refused with reason 'off tick' instead of being silently rounded. The JS matcher accepts any bigint, so
// this is the one semantic difference between the two backends, and it is deliberate.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const BIN = path.resolve(HERE, '../native/book/bookd');
const OP = { ADD: 1, CANCEL: 2, DEPTH: 3, BEST: 4, ORDERS: 5, PREVIEW: 6, GET: 7, STAT: 8, SYNC: 9 };
const REASON = [null, 'duplicate', 'bad order', 'would cross', 'off tick', 'price out of range', 'size overflow', 'book full'];
const NO_TICK = -(2n ** 63n);
const SLEEP = new Int32Array(new SharedArrayBuffer(4));

class Frame {                      // a growable little-endian writer
  constructor(n = 256) { this.b = Buffer.alloc(n); this.i = 0; }
  need(n) { if (this.i + n > this.b.length) { const b = Buffer.alloc(Math.max(this.b.length * 2, this.i + n)); this.b.copy(b); this.b = b; } }
  u8(v) { this.need(1); this.b[this.i++] = v; return this; }
  u32(v) { this.need(4); this.b.writeUInt32LE(v >>> 0, this.i); this.i += 4; return this; }
  u64(v) { this.need(8); this.b.writeBigUInt64LE(BigInt(v) & 0xffffffffffffffffn, this.i); this.i += 8; return this; }
  i64(v) { this.need(8); this.b.writeBigInt64LE(BigInt(v), this.i); this.i += 8; return this; }
  u128(v) { return this.u64(BigInt(v) & 0xffffffffffffffffn).u64(BigInt(v) >> 64n); }
  done() { return this.b.subarray(0, this.i); }
}
class Cursor {                     // a little-endian reader over one reply payload
  constructor(b, i = 0) { this.b = b; this.i = i; }
  u8() { return this.b[this.i++]; }
  u32() { const v = this.b.readUInt32LE(this.i); this.i += 4; return v; }
  u64() { const v = this.b.readBigUInt64LE(this.i); this.i += 8; return v; }
  i64() { const v = this.b.readBigInt64LE(this.i); this.i += 8; return v; }
  u128() { const lo = this.u64(), hi = this.u64(); return (hi << 64n) | lo; }
}

/// a bookd process plus the synchronous framing over its FIFOs
function connect({ bin = BIN, dir = null, journal = null, tickSize = 10n ** 14n, tickLimit = 10000, fsync = true, timeoutMs = 10000, log = null } = {}) {
  if (!fs.existsSync(bin)) throw new Error(`native book binary missing: ${bin} (run make -C native/book)`);
  const tmp = dir || fs.mkdtempSync(path.join(os.tmpdir(), 'rollbook-'));
  fs.mkdirSync(tmp, { recursive: true });
  const req = path.join(tmp, 'req.fifo'), res = path.join(tmp, 'res.fifo');
  for (const f of [req, res]) { try { fs.unlinkSync(f); } catch {} execFileSync('mkfifo', [f]); }
  const args = ['--fifo', req, res, '--tick-size', String(tickSize), '--tick-limit', String(tickLimit), '--quiet'];
  args.push('--journal', journal || path.join(tmp, 'book.journal'));
  if (!fsync) args.push('--fsync', '0');
  const child = spawn(bin, args, { stdio: ['ignore', 'ignore', log ? 'pipe' : 'ignore'] });
  if (log && child.stderr) child.stderr.on('data', (d) => log(`[bookd] ${String(d).trim()}`));
  // open our write end first (O_RDWR never blocks on a FIFO), then the read end non-blocking so a dead bookd
  // surfaces as an error instead of a hang
  const reqFd = fs.openSync(req, fs.constants.O_RDWR | fs.constants.O_NONBLOCK);
  const resFd = fs.openSync(res, fs.constants.O_RDWR | fs.constants.O_NONBLOCK);

  let rbuf = Buffer.alloc(1 << 16), have = 0, at = 0;
  const compact = () => { if (at > 0) { rbuf.copy(rbuf, 0, at, have); have -= at; at = 0; } };
  const alive = () => { if (child.exitCode !== null) throw new Error(`bookd exited with ${child.exitCode}`); };
  function readOnce() {                                 // one non-blocking read; false if nothing was there yet
    compact();
    if (have >= rbuf.length) { const b = Buffer.alloc(rbuf.length * 2); rbuf.copy(b); rbuf = b; }
    try { const got = fs.readSync(resFd, rbuf, have, rbuf.length - have); if (got > 0) { have += got; return true; } }
    catch (e) { if (e.code !== 'EAGAIN') throw e; }
    return false;
  }
  function pack(payloads) {
    let total = 0;
    for (const p of payloads) total += 4 + p.length;
    const out = Buffer.alloc(total);
    let o = 0;
    for (const p of payloads) { out.writeUInt32LE(p.length, o); o += 4; p.copy(out, o); o += p.length; }
    return out;
  }
  /// write the batch and read its replies, interleaved. Both FIFOs hold 64 KB, and a batch of a few thousand
  /// orders is larger than that in both directions, so writing the whole request before reading any reply
  /// deadlocks: bookd blocks writing replies while we block writing requests. Alternating is what makes a
  /// batch of any size safe. Cursors are handed out only after every byte has arrived, because a read may
  /// compact or grow the buffer; offsets are kept relative to `at`, which compaction adjusts.
  function call(payloads) {
    const out = pack(payloads), need = payloads.length;
    let woff = 0, rel = 0, spins = 0;
    const offs = []; const t0 = Date.now();
    for (;;) {
      while (offs.length < need && have - at - rel >= 4) {
        const len = rbuf.readUInt32LE(at + rel);
        if (have - at - rel < 4 + len) break;
        offs.push(rel + 4); rel += 4 + len;
      }
      if (offs.length === need) break;
      let progress = false;
      if (woff < out.length) {
        try { const w = fs.writeSync(reqFd, out, woff, out.length - woff); woff += w; progress = w > 0; }
        catch (e) { if (e.code !== 'EAGAIN') throw e; }
      }
      if (readOnce()) progress = true;
      if (!progress) {
        alive();
        if (Date.now() - t0 > timeoutMs) throw new Error('bookd timed out');
        if (++spins > 2000) Atomics.wait(SLEEP, 0, 0, 1);   // after a short spin, stop burning the core
      } else spins = 0;
    }
    const cursors = offs.map((o) => new Cursor(rbuf, at + o));
    at += rel;
    return cursors.map((c) => ({ op: c.u8(), status: c.u8(), seq: c.u64(), c }));
  }
  function close() {
    try { fs.closeSync(reqFd); } catch {}
    try { fs.closeSync(resFd); } catch {}
    try { child.kill('SIGTERM'); } catch {}
    if (!dir) { for (const f of [req, res]) { try { fs.unlinkSync(f); } catch {} } }
  }
  return { call, close, child, dir: tmp };
}

/// the same shape createBook() returns, plus addMany/stat/sync/close
export function createNativeBook(opts = {}) {
  const tickSize = BigInt(opts.tickSize ?? 10n ** 14n);
  const io = connect({ ...opts, tickSize });
  const ids = new Map();            // hash → id
  const hashes = new Map();         // id → hash
  const uids = new Map();           // user → uid
  const users = new Map();          // uid → user
  let nextId = 0n, nextUid = 0n, resting = 0, seq = 0n;

  const idOf = (hash) => { let i = ids.get(hash); if (i === undefined) { i = ++nextId; ids.set(hash, i); hashes.set(i, hash); } return i; };
  const uidOf = (user) => { let i = uids.get(user); if (i === undefined) { i = ++nextUid; uids.set(user, i); users.set(i, user); } return i; };
  const drop = (id) => { const h = hashes.get(id); if (h !== undefined) { hashes.delete(id); ids.delete(h); } };
  const px = (tick) => tick * tickSize;
  const tickOf = (price) => (price % tickSize === 0n ? price / tickSize : null);

  function order(c) {               // the ORDER body shared by cancel/get/orders
    const found = c.u8() === 1;
    const id = c.u64(), uid = c.u64(), oseq = c.u64(), tick = c.i64();
    const size = c.u128(), remaining = c.u128(), flags = c.u32(), buy = c.u8() === 1;
    if (!found) return null;
    return { hash: hashes.get(id) ?? null, user: users.get(uid) ?? null, buy, price: px(tick), size, remaining,
             postOnly: (flags & 1) !== 0, ioc: (flags & 2) !== 0, seq: Number(oseq), ts: 0 };
  }

  function addFrame(o) {
    const tick = tickOf(BigInt(o.price));
    if (tick === null) return null;
    const f = new Frame(48);
    f.u8(OP.ADD).u64(idOf(o.hash)).u64(uidOf(o.user)).u8(o.buy ? 1 : 0).u8(0)
     .u32((o.postOnly ? 1 : 0) | (o.ioc ? 2 : 0)).i64(tick).u128(BigInt(o.size));
    return f.done();
  }
  function addReply(o, r) {
    seq = r.seq;
    const remaining = r.c.u128(), nfills = r.c.u32(), rested = r.c.u8() === 1, reason = r.c.u8();
    const fills = [];
    for (let k = 0; k < nfills; k++) {
      const fseq = r.c.u64(), makerId = r.c.u64(), takerId = r.c.u64(), makerUid = r.c.u64(), takerUid = r.c.u64();
      const tick = r.c.i64(), size = r.c.u128(), makerRem = r.c.u128(), takerBuys = r.c.u8() === 1;
      fills.push({ seq: Number(fseq), makerHash: hashes.get(makerId), takerHash: hashes.get(takerId),
                   maker: users.get(makerUid), taker: users.get(takerUid), price: px(tick), size, takerBuys });
      if (makerRem === 0n) { drop(makerId); resting--; }
    }
    if (rested) resting++; else if (reason !== 1) drop(idOf(o.hash));   // a duplicate must keep the resting order's id
    return { fills, rested, remaining, reason: REASON[reason] ?? null };
  }

  function add(o) {
    const f = addFrame(o);
    if (!f) return { fills: [], rested: false, remaining: BigInt(o.size), reason: 'off tick' };
    return addReply(o, io.call([f])[0]);
  }
  /// a whole arrival batch in one write, one journal fsync and one read: the only way the process boundary pays
  function addMany(list) {
    const frames = [], out = [], skipped = [];
    for (const o of list) { const f = addFrame(o); if (f) { frames.push(f); out.push(o); } else skipped.push(o); }
    const replies = frames.length ? io.call(frames) : [];
    const res = out.map((o, i) => addReply(o, replies[i]));
    for (const o of skipped) res.push({ fills: [], rested: false, remaining: BigInt(o.size), reason: 'off tick' });
    return res;
  }
  function cancel(hash) {
    const id = ids.get(hash); if (id === undefined) return null;
    const r = io.call([new Frame(16).u8(OP.CANCEL).u64(id).done()])[0];
    seq = r.seq;
    const o = order(r.c);
    if (o) { drop(id); resting--; }
    return o;
  }
  function depth(n = 25) {
    const r = io.call([new Frame(8).u8(OP.DEPTH).u32(n).done()])[0];
    seq = r.seq;
    const nb = r.c.u32(), na = r.c.u32(), lv = () => ({ price: px(r.c.i64()), size: r.c.u128(), orders: r.c.u32() });
    const bids = []; for (let i = 0; i < nb; i++) bids.push(lv());
    const asks = []; for (let i = 0; i < na; i++) asks.push(lv());
    return { bids, asks, seq: Number(seq) };
  }
  function best() {
    const r = io.call([new Frame(4).u8(OP.BEST).done()])[0]; seq = r.seq;
    const bid = r.c.i64(), ask = r.c.i64();
    return { bid: bid === NO_TICK ? null : px(bid), ask: ask === NO_TICK ? null : px(ask) };
  }
  function get(hash) {
    const id = ids.get(hash); if (id === undefined) return null;
    const r = io.call([new Frame(16).u8(OP.GET).u64(id).done()])[0]; seq = r.seq;
    return order(r.c);
  }
  function orders(user = null) {
    const uid = user === null ? 0n : uids.get(user);
    if (user !== null && uid === undefined) return [];
    const r = io.call([new Frame(16).u8(OP.ORDERS).u64(uid ?? 0n).u8(user === null ? 0 : 1).done()])[0];
    seq = r.seq;
    const n = r.c.u32(), out = [];
    for (let i = 0; i < n; i++) { const o = order(r.c); if (o) out.push(o); }
    return out;
  }
  function preview(buy, price, size, user = null) {
    const tick = tickOf(BigInt(price));
    if (tick === null) return { filled: 0n, cost: 0n, fills: 0, avg: null };
    const uid = user === null ? 0n : uids.get(user);
    const f = new Frame(40).u8(OP.PREVIEW).u8(buy ? 1 : 0).u8(user !== null && uid !== undefined ? 1 : 0)
      .i64(tick).u128(BigInt(size)).u64(uid ?? 0n);
    const r = io.call([f.done()])[0]; seq = r.seq;
    const filled = r.c.u128();
    let cost = 0n;
    for (let i = 0; i < 4; i++) cost |= r.c.u64() << BigInt(64 * i);
    const fills = r.c.u32();
    return { filled, cost, fills, avg: filled > 0n ? cost / filled : null };
  }
  /// what the next task (the Kafka-replicated miner log) votes on: the sequence number, the transcript hash of
  /// every applied request, and a hash of the book itself
  function stat() {
    const r = io.call([new Frame(4).u8(OP.STAT).done()])[0]; seq = r.seq;
    return { seq: r.seq, resting: Number(r.c.u64()), stateHash: r.c.u64(), bookHash: r.c.u64(), journal: Number(r.c.u64()),
             adds: Number(r.c.u64()), fills: Number(r.c.u64()), cancels: Number(r.c.u64()), rejects: Number(r.c.u64()),
             tickSize: r.c.i64(), tickLimit: r.c.i64() };
  }
  const sync = () => { const r = io.call([new Frame(4).u8(OP.SYNC).done()])[0]; seq = r.seq; return Number(r.c.u64()); };

  return { add, addMany, cancel, depth, best, get, orders, preview, stat, sync,
           close: io.close, dir: io.dir, native: true,
           get size() { return resting; }, get seq() { return Number(seq); } };
}
