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
//
// createNativeCommit() is the second client in this file: the shard's incremental state commitment
// (native/book/commit.hpp, L3-NATIVE-BOOK.md §9) served by the same bookd behind OP_COMMIT / OP_CPROOF. Its
// updates are BUFFERED and go out as one request per flush — a whole batch's inserts, fills and cancels in a
// few frames and one round trip, the root read in the same reply — because the round trip (~25 µs) is what
// the process boundary costs and a 2,000-op batch must not pay it 2,000 times. It is owned by
// engine/l3/miner/commit-state.js, which decides between it and the in-process JS tree.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { decodeCommitProof } from './miner/commit.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const BIN = path.resolve(HERE, '../native/book/bookd');
const OP = { ADD: 1, CANCEL: 2, DEPTH: 3, BEST: 4, ORDERS: 5, PREVIEW: 6, GET: 7, STAT: 8, SYNC: 9, COMMIT: 10, CPROOF: 11 };
const CK = { INSERT: 1, REMOVE: 2, SET: 3 };
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
  raw(u) { this.need(u.length); this.b.set(u, this.i); this.i += u.length; return this; }
  /// a 256-bit unsigned as 32 big-endian bytes (what commit.hpp's Entry carries for price and remaining)
  u256be(v) {
    this.need(32); let x = BigInt(v);
    if (x < 0n || x >> 256n) throw new Error('u256 out of range');
    for (let k = 3; k >= 0; k--) { this.b.writeBigUInt64BE(x & 0xffffffffffffffffn, this.i + 8 * k); x >>= 64n; }
    this.i += 32; return this;
  }
  done() { return this.b.subarray(0, this.i); }
}
class Cursor {                     // a little-endian reader over one reply payload
  constructor(b, i = 0) { this.b = b; this.i = i; }
  u8() { return this.b[this.i++]; }
  u32() { const v = this.b.readUInt32LE(this.i); this.i += 4; return v; }
  u64() { const v = this.b.readBigUInt64LE(this.i); this.i += 8; return v; }
  i64() { const v = this.b.readBigInt64LE(this.i); this.i += 8; return v; }
  u128() { const lo = this.u64(), hi = this.u64(); return (hi << 64n) | lo; }
  raw(n) { const u = new Uint8Array(this.b.buffer, this.b.byteOffset + this.i, n).slice(); this.i += n; return u; }
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
  // a helper process must never keep this one alive: a book or a commit tree nobody closed would otherwise hold
  // the event loop open after the last test (bookd exits by itself when our end of its request FIFO goes away)
  child.unref(); if (child.stderr) child.stderr.unref?.();
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
                   maker: users.get(makerUid), taker: users.get(takerUid), price: px(tick), size, takerBuys, makerRemaining: makerRem });
      if (makerRem === 0n) { drop(makerId); resting--; }
    }
    if (rested) resting++; else if (reason !== 1) drop(idOf(o.hash));   // a duplicate must keep the resting order's id
    // the order's own sequence number: the book numbers the order before its fills (book.cpp add()), and the
    // reply's seq is the book's after them. 0 when the order was never numbered (duplicate, bad, off tick).
    const own = reason === 0 || reason === 3 ? Number(r.seq) - nfills : 0;
    return { fills, rested, remaining, reason: REASON[reason] ?? null, seq: own };
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

// ------------------------------------------------------------------------------------- the commit tree client
/**
 * createNativeCommit({ bin?, dir?, log?, flushAt? }) — native/book/commit.hpp behind bookd, buffered.
 *
 *   insert(e)              e: { hash, user, buy, price, remaining, seq } — a resting order (restingLeaf's fields)
 *   remove(hash)           a cancel that hit, or a maker filled out
 *   setRemaining(hash, r)  a partial maker fill
 *   flush()                send what is buffered: as many frames as needed (≤ flushAt updates each, well under
 *                          bookd's 1 MiB frame cap), ONE round trip; returns { applied, refused, root, size }
 *   root()                 flush + the root, in the same round trip (a COMMIT with the buffered updates, or n = 0)
 *   proof(hash)            flush, then the inclusion proof as commit.js's decodeCommitProof shapes it, or null
 *   size()                 flush + the tree's size
 *   close()
 *
 * The buffer is a plain ordered list; coalescing (a fill after an insert of the same order in one batch, an
 * insert cancelled within the batch) is commit-state.js's job, where it is shared with the JS tree.
 */
export function createNativeCommit({ bin = BIN, dir = null, log = null, flushAt = 4000, timeoutMs = 30000 } = {}) {
  const io = connect({ bin, dir, journal: 'none', log, timeoutMs });   // no book, no journal: the tree is derived state
  let frames = [], cur = null, n = 0, buffered = 0, last = { applied: 0, refused: 0, root: ZERO32_HEX, size: 0 };
  const hashBytes = (h) => { const u = Buffer.from(String(h).startsWith('0x') ? h.slice(2) : h, 'hex'); if (u.length !== 32) throw new Error(`commit: hash of ${u.length} bytes`); return u; };
  const userBytes = (a) => { const u = Buffer.from(String(a).startsWith('0x') ? a.slice(2) : a, 'hex'); if (u.length !== 20) throw new Error(`commit: user of ${u.length} bytes`); return u; };
  function frame() { if (!cur) { cur = new Frame(1 << 16); cur.u8(OP.COMMIT).u32(0); n = 0; } return cur; }
  function seal() { if (cur) { cur.b.writeUInt32LE(n, 1); frames.push(cur.done()); cur = null; n = 0; } }
  const bump = () => { buffered++; if (++n >= flushAt) seal(); };
  function insert(e) { frame().u8(CK.INSERT).raw(hashBytes(e.hash)).raw(userBytes(e.user)).u8(e.buy ? 1 : 0).u256be(e.price).u256be(e.remaining).u64(e.seq); bump(); }
  function remove(hash) { frame().u8(CK.REMOVE).raw(hashBytes(hash)); bump(); }
  function setRemaining(hash, remaining) { frame().u8(CK.SET).raw(hashBytes(hash)).u256be(remaining); bump(); }
  /// everything buffered, in one round trip; an empty buffer still asks for the root
  function flush() {
    seal();
    if (!frames.length) frames.push(new Frame(8).u8(OP.COMMIT).u32(0).done());
    const replies = io.call(frames); frames = []; buffered = 0;
    let applied = 0, refused = 0, root = ZERO32_HEX, size = 0;
    for (const r of replies) {
      if (r.status !== 0) throw new Error('bookd refused a COMMIT frame');
      applied += r.c.u32(); refused += r.c.u32(); root = '0x' + Buffer.from(r.c.raw(32)).toString('hex'); size = Number(r.c.u64());
    }
    last = { applied, refused, root, size };
    return last;
  }
  function root() { return flush().root; }
  function size() { return flush().size; }
  function proof(hash) {
    flush();
    const r = io.call([new Frame(40).u8(OP.CPROOF).raw(hashBytes(hash)).done()])[0];
    if (r.status !== 0) throw new Error('bookd refused a CPROOF frame');
    const len = r.c.u32(); if (len === 0xffffffff) return null;
    return decodeCommitProof(r.c.raw(len), String(hash).toLowerCase());
  }
  return { insert, remove, setRemaining, flush, root, size, proof, close: io.close, native: true, get buffered() { return buffered; }, get last() { return last; } };
}
const ZERO32_HEX = '0x' + '00'.repeat(32);
