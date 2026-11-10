// engine/l3/miner/log.js — the replicated log the L3 miners read, behind one interface and two implementations.
//
//   append(topic, msg)                         → offset of the appended record
//   appendMany(topic, msgs)                    → offset of the FIRST record
//   read(topic, fromOffset, limit)             → [{ offset, value }]
//   subscribe(topic, fromOffset, handler)      → { close() }   handler({ offset, value }), awaited, in order
//   offset(topic)                              → how many records the topic holds (= the next offset)
//   close()
//
// An offset is the record's index in the topic, from 0. It is dense and monotonic in both implementations, so a
// miner's journal can say "I am at offset N" and resume from N+1 on either one.
//
// FileLog — one append-only JSONL file per topic under `dir`. This is the test and single-node implementation,
// and it is a complete log: the engine plus three local miners is a working quorum with no broker at all. A
// subscriber tails the file by byte position and only ever dispatches COMPLETE lines, so a reader that catches
// an appender mid-write waits rather than parsing half a record. In-process appends wake the tailers
// immediately (setImmediate) instead of waiting for the poll, which is what makes the cluster test run at the
// speed of the matcher rather than the speed of a timer.
//
// KafkaLog — kafkajs. ONE PARTITION per topic, always: the partition order IS the consensus (docs/L3-MINERS.md
// §2), and a second partition would make "the sequence" ambiguous. The producer runs with acks: -1 (all
// in-sync replicas) because an order the sequencer acked but the log lost is the one failure the design cannot
// tolerate. kafkajs is imported dynamically so that nothing in the engine, the tests or the browser build pays
// for it — and so that `L3_LOG=file` works in an environment where the package is not installed at all.
//
// Both implementations are at-least-once on the consumer side: a handler can see the same offset twice after a
// restart (FileLog cannot, Kafka can, depending on commit timing), so every handler in this directory is
// idempotent by offset. The miner keeps `lastOffset` in its journal and ignores anything it has already applied.
import fs from 'node:fs';
import path from 'node:path';

const SAFE = (t) => String(t).replace(/[^a-zA-Z0-9._-]/g, '_');
/// bigints are everywhere in this codebase (prices, sizes) and JSON.stringify refuses them. The log's records
/// are already built from strings by the sequencer, but a stray bigint should not take the shard down.
const enc = (v) => JSON.stringify(v, (_k, x) => (typeof x === 'bigint' ? x.toString() : x));

// ------------------------------------------------------------------------------------------------- FileLog
export function createFileLog({ dir, pollMs = 5, fsync = false, logger = null } = {}) {
  if (!dir) throw new Error('FileLog needs a dir');
  fs.mkdirSync(dir, { recursive: true });
  const topics = new Map();   // topic → { file, fd, count, bytes }
  const tails = new Set();    // live subscriptions
  let timer = null, closed = false;

  // `create` false makes this a pure reader: a topic nobody has written does not come into being because someone
  // looked at it. That is not a nicety — a shard's log is deleted when its market resolves and its quorum has
  // finalised every index, and a reader that creates on open resurrects the topic (empty, still holding the
  // broker's per-topic floor) the moment any miner sweeps it. See ensureTopic below: the shard's owner creates.
  function open(topic, create = true) {
    const key = SAFE(topic); let t = topics.get(key);
    if (!t) {
      const file = path.join(dir, `${key}.jsonl`);
      let count = 0, bytes = 0, found = false;
      try { const buf = fs.readFileSync(file); bytes = buf.length; found = true; for (let i = 0; i < buf.length; i++) if (buf[i] === 10) count++; } catch {}
      if (!found && !create) return null;
      t = { file, fd: fs.openSync(file, 'a'), count, bytes };
      topics.set(key, t);
    }
    return t;
  }
  function write(topic, msgs) {
    const t = open(topic);
    let s = ''; for (const m of msgs) s += enc(m) + '\n';
    const buf = Buffer.from(s, 'utf8');
    fs.writeSync(t.fd, buf);
    if (fsync) fs.fsyncSync(t.fd);
    const first = t.count; t.count += msgs.length; t.bytes += buf.length;
    wake();
    return first;
  }
  const wake = () => { for (const tl of tails) setImmediate(() => pump(tl)); };

  /// one tail: a byte position, a line counter and a serialised handler. `busy` guarantees the handler is never
  /// re-entered, so a slow miner simply reads later — it never sees two batches at once or out of order.
  function makeTail(topic, fromOffset, handler, create = false) {
    const t = open(topic, create);
    // A topic that does not exist yet still gets a tail: it resolves its file on the first poll that finds one. That
    // is what lets a reader WAIT without creating — the engine's quorum subscribes to a shard's votes topic before
    // any miner has voted, and a dead handle there would starve finality for good.
    const file = t ? t.file : path.join(dir, `${SAFE(topic)}.jsonl`);
    return { topic, file, pos: 0, offset: 0, from: Math.max(0, fromOffset | 0), rest: '', busy: false, closed: false, handler, errors: 0, waiting: !t };
  }
  async function pump(tl) {
    if (tl.busy || tl.closed || closed) return;
    tl.busy = true;
    try {
      for (;;) {
        let stat; try { stat = fs.statSync(tl.file); } catch { break; }   // not there yet: a waiting tail just idles
        if (tl.waiting) tl.waiting = false;
        if (stat.size <= tl.pos) break;
        const len = Math.min(stat.size - tl.pos, 1 << 20);
        const buf = Buffer.allocUnsafe(len);
        let got = 0;
        const fd = fs.openSync(tl.file, 'r');
        try { got = fs.readSync(fd, buf, 0, len, tl.pos); } finally { fs.closeSync(fd); }
        if (got <= 0) break;
        tl.pos += got;
        tl.rest += buf.subarray(0, got).toString('utf8');
        let nl;
        while ((nl = tl.rest.indexOf('\n')) >= 0) {
          const line = tl.rest.slice(0, nl); tl.rest = tl.rest.slice(nl + 1);
          const offset = tl.offset++;
          if (offset < tl.from || !line) continue;
          let value; try { value = JSON.parse(line); } catch { tl.errors++; logger && logger(`[l3log] ${tl.topic}@${offset}: unparseable record skipped`); continue; }
          try { await tl.handler({ offset, value, topic: tl.topic }); } catch (e) { tl.errors++; logger && logger(`[l3log] handler failed on ${tl.topic}@${offset}: ${e.message}`); }
          if (tl.closed) return;
        }
      }
    } finally { tl.busy = false; }
  }

  return {
    kind: 'file', dir,
    async append(topic, msg) { return write(topic, [msg]); },
    async appendMany(topic, msgs) { return msgs.length ? write(topic, msgs) : (open(topic, false)?.count ?? 0); },
    async offset(topic) { return open(topic, false)?.count ?? 0; },
    async read(topic, fromOffset = 0, limit = Infinity) {
      const t = open(topic, false); const out = []; if (!t) return out;
      let buf; try { buf = fs.readFileSync(t.file, 'utf8'); } catch { return out; }
      let i = 0;
      for (const line of buf.split('\n')) {
        if (!line) { continue; }
        const offset = i++;
        if (offset < fromOffset) continue;
        if (out.length >= limit) break;
        try { out.push({ offset, value: JSON.parse(line), topic }); } catch {}
      }
      return out;
    },
    exists(topic) { return !!open(topic, false); },
    /// the shard's OWNER creates its topics, explicitly, once. Readers never do (see open).
    async ensureTopic(topic) { open(topic, true); return true; },
    async subscribe(topic, fromOffset, handler, { create = false } = {}) {
      const tl = makeTail(topic, fromOffset, handler, create);
      tails.add(tl);
      if (!timer) { timer = setInterval(() => { for (const x of tails) pump(x); }, pollMs); timer.unref?.(); }
      await pump(tl);
      return { waiting: !!tl.waiting, topic, close() { tl.closed = true; tails.delete(tl); if (!tails.size && timer) { clearInterval(timer); timer = null; } }, stats: () => ({ offset: tl.offset, errors: tl.errors, waiting: !!tl.waiting }) };
    },
    async close() {
      closed = true;
      for (const tl of tails) tl.closed = true; tails.clear();
      if (timer) { clearInterval(timer); timer = null; }
      for (const t of topics.values()) { try { fs.closeSync(t.fd); } catch {} }
      topics.clear();
    },
  };
}

// ------------------------------------------------------------------------------------------------ KafkaLog
/// brokers: 'host:9092,host2:9092' or an array. clientId identifies this process in the broker's logs.
/// groupPrefix keeps two miners on one broker from sharing a consumer group — each subscription gets its own
/// group, because every miner must see EVERY record, not a share of them (this is a replicated log, not a work
/// queue; the earlier concept's `group.id: 'validators'` load-balanced orders across validators, which is
/// exactly what must NOT happen here).
export async function createKafkaLog({ brokers, clientId = 'rolla-l3', groupPrefix = 'rolla-l3', ssl = undefined, sasl = undefined, logger = null, partitions = 1, replication = 1, acks = -1, connectionTimeout = 10000, retries = 2, readTimeoutMs = 15000, pollWaitMs = 15000 } = {}) {
  const list = Array.isArray(brokers) ? brokers : String(brokers || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (!list.length) throw new Error('KafkaLog needs L3_KAFKA_BROKERS');
  let Kafka, logLevel;
  try { ({ Kafka, logLevel } = await import('kafkajs')); }
  catch (e) { throw new Error(`kafkajs is not installed (npm i kafkajs) — use L3_LOG=file instead: ${e.message}`); }
  // a bounded retry: an engine booting against a dead broker must fail in seconds and say so, not spend half a
  // minute in kafkajs's exponential backoff before anyone learns the log is unavailable
  const kafka = new Kafka({ clientId, brokers: list, ssl, sasl, connectionTimeout, logLevel: logLevel.ERROR, retry: { retries: Number(retries) } });
  const producer = kafka.producer({ allowAutoTopicCreation: true, idempotent: true, maxInFlightRequests: 1 });
  await producer.connect();
  const admin = kafka.admin(); await admin.connect();
  const known = new Set(); const consumers = new Set();
  /// does the broker hold this topic? `known` is only ever a positive cache — a topic that is absent must be
  /// re-checked, because the shard's owner may create it at any moment.
  async function exists(topic) {
    if (known.has(topic)) return true;
    try { const list = await admin.listTopics(); if (list.includes(topic)) { known.add(topic); return true; } } catch { }
    return false;
  }

  /// one partition, always. If the topic exists with more, say so loudly rather than sequencing into ambiguity.
  async function ensure(topic) {
    if (known.has(topic)) return;
    try { await admin.createTopics({ topics: [{ topic, numPartitions: partitions, replicationFactor: replication }], waitForLeaders: true }); } catch {}
    try {
      const md = await admin.fetchTopicMetadata({ topics: [topic] });
      const n = md.topics[0]?.partitions?.length || 1;
      if (n !== 1) throw new Error(`topic ${topic} has ${n} partitions; the L3 log needs exactly 1 (the partition order is the consensus)`);
    } catch (e) { if (/needs exactly 1/.test(e.message)) throw e; }
    known.add(topic);
  }
  /// a topic that was just auto-created has no leader for a moment and the broker answers "This server does not
  /// host this topic-partition" (or LEADER_NOT_AVAILABLE); a fresh shard's first offset/append/subscribe lands
  /// exactly there, so those wait for leadership instead of failing the shard
  const NO_LEADER = /does not host this topic-partition|LEADER_NOT_AVAILABLE|NOT_LEADER|UNKNOWN_TOPIC_OR_PARTITION|not the leader/i;
  async function withLeader(what, fn, attempts = 20) {
    for (let i = 0; ; i++) {
      try { return await fn(); }
      catch (e) { if (!NO_LEADER.test(e.message || '') || i >= attempts - 1) throw e; if (i === 0) logger && logger(`[l3log] ${what}: waiting for the partition leader (${e.message.slice(0, 60)})`); await new Promise((r) => setTimeout(r, 1000)); }
    }
  }
  async function send(topic, msgs) {
    await ensure(topic);
    const r = await withLeader(`produce ${topic}`, () => producer.send({ topic, acks, messages: msgs.map((m) => ({ key: String(m.shard ?? m.index ?? ''), value: enc(m) })) }));
    const base = r?.[0]?.baseOffset ?? r?.[0]?.offset;
    return base != null ? Number(base) : -1;
  }

  const self = {
    kind: 'kafka', brokers: list,
    async append(topic, msg) { return send(topic, [msg]); },
    async appendMany(topic, msgs) { return msgs.length ? send(topic, msgs) : self.offset(topic); },
    async offset(topic, { create = false } = {}) {
      if (create) await ensure(topic);
      else if (!(await exists(topic))) return 0;
      const o = await withLeader(`offsets ${topic}`, () => admin.fetchTopicOffsets(topic));
      return Number(o?.[0]?.high ?? 0);
    },
    exists,
    /// the shard's OWNER creates its topics, explicitly, once — readers never do
    async ensureTopic(topic) { await ensure(topic); known.add(topic); return true; },
    /// watermark-driven: the records the topic holds NOW from `fromOffset` (at most `limit`), returned as soon as
    /// they are all in — not after a fixed window. A new consumer group's first rebalance can take seconds on a
    /// broker (group.initial.rebalance.delay), longer than the 3 s this used to wait: the sequencer's tail read
    /// (resume()) came back empty and a restarted shard would have started a second chain from index 0. Verified
    /// against the Fly broker by log-kafka.test.mjs. A read that is still short at readTimeoutMs returns what it has.
    async read(topic, fromOffset = 0, limit = 500) {
      const high = await self.offset(topic); const from = Math.max(0, Number(fromOffset) || 0);
      const want = Math.min(Number(limit) || 0, Math.max(0, high - from)); if (want <= 0) return [];
      const out = []; const seen = new Set(); let sub = null, done = false, timer = null;
      await new Promise((resolve, reject) => {
        const finish = () => { if (done) return; done = true; if (timer) clearTimeout(timer); resolve(); };
        self.subscribe(topic, from, async ({ offset, value }) => {
          if (done || offset < from || offset >= from + want || seen.has(offset)) return;
          seen.add(offset); out.push({ offset, value, topic }); if (out.length >= want) finish();
        }).then((s) => { sub = s; if (done) return; timer = setTimeout(() => { logger && logger(`[l3log] read ${topic}@${from}: ${out.length}/${want} record(s) within ${readTimeoutMs} ms`); finish(); }, readTimeoutMs); }, reject);
      });
      if (sub) sub.close().catch(() => {});
      return out.sort((a, b) => a.offset - b.offset);
    },
    /// delete a topic (scratch topics of a check; never a shard's)
    async drop(topic) { known.delete(topic); await admin.deleteTopics({ topics: [topic] }); },
    /// `create` false makes this a pure reader. A shard's log is deleted once its market has resolved and its quorum
    /// has finalised every index; a reader that creates on subscribe resurrects that topic — empty, and still holding
    /// the broker's per-topic floor — the moment any miner sweeps it, which is why deleting them by hand never stuck.
    /// kafkajs's consumer auto-creates too, so BOTH that flag and `ensure` have to go for a reader.
    async subscribe(topic, fromOffset, handler, { create = false } = {}) {
      if (create) await ensure(topic);
      else if (!(await exists(topic))) {
        // Wait for it rather than hand back a dead handle. A reader must not create the topic — that is what let a
        // retired shard's log come back, empty and still holding the broker's per-topic floor — but it must also not
        // give up: the engine's quorum subscribes to a shard's votes topic before any miner has voted, and a shard
        // the sequencer boots a moment later would be stranded. The probe backs off to pollWaitMs, so a shard that
        // is retired for good costs one cheap listTopics per interval and never comes back.
        let wait = 250, timer = null, real = null, closed2 = false;
        const tryAttach = async () => {
          if (closed2) return;
          try {
            if (await exists(topic)) { real = await self.subscribe(topic, fromOffset, handler, { create: false }); if (closed2) real.close(); return; }
          } catch { }
          wait = Math.min(pollWaitMs, wait * 2);
          timer = setTimeout(tryAttach, wait); timer.unref?.();
        };
        timer = setTimeout(tryAttach, wait); timer.unref?.();
        return { waiting: true, topic, close() { closed2 = true; if (timer) clearTimeout(timer); if (real) real.close(); }, stats: () => (real ? real.stats() : { offset: -1, errors: 0, waiting: true }) };
      }
      const groupId = `${groupPrefix}-${topic}-${Math.random().toString(36).slice(2, 10)}`;
      // a batch of 2,000 orders is ~1.2 MB; kafkajs's default fetch (1 MiB per partition) then carries ONE batch per
      // round trip, and a miner far from the broker pays that round trip per batch on top of its own work. Fetch
      // up to 16 MiB (8 MiB per partition, the broker's message limit) so one trip carries several batches.
      const consumer = kafka.consumer({ groupId, sessionTimeout: 30000, allowAutoTopicCreation: !!create, maxBytesPerPartition: 8 << 20, maxBytes: 16 << 20, maxWaitTimeInMs: 50 });
      await consumer.connect();
      await withLeader(`subscribe ${topic}`, () => consumer.subscribe({ topic, fromBeginning: true }));
      let seeked = false;
      await consumer.run({
        eachMessage: async ({ message, partition }) => {
          const offset = Number(message.offset);
          if (offset < fromOffset) return;
          let value; try { value = JSON.parse(message.value.toString('utf8')); } catch { return; }
          await handler({ offset, value, topic, partition });
        },
      });
      // seek must happen after run(); a consumer that joins the group fresh would otherwise re-read from 0
      if (fromOffset > 0 && !seeked) { try { consumer.seek({ topic, partition: 0, offset: String(fromOffset) }); seeked = true; } catch (e) { logger && logger(`[l3log] seek failed on ${topic}: ${e.message}`); } }
      consumers.add(consumer);
      return { async close() { consumers.delete(consumer); try { await consumer.disconnect(); } catch {} } };
    },
    async close() {
      for (const c of consumers) { try { await c.disconnect(); } catch {} }
      consumers.clear();
      try { await producer.disconnect(); } catch {}
      try { await admin.disconnect(); } catch {}
    },
  };
  return self;
}

/// pick an implementation from the environment: L3_LOG=file|kafka (default file), L3_LOG_DIR, L3_KAFKA_BROKERS.
export async function createLog({ env = process.env, dir = null, clientId = 'rolla-l3', logger = null } = {}) {
  const kind = (env.L3_LOG || 'file').toLowerCase();
  if (kind === 'kafka' || kind === 'redpanda') {
    return createKafkaLog({ brokers: env.L3_KAFKA_BROKERS || '127.0.0.1:9092', clientId, logger,
      replication: Number(env.L3_KAFKA_REPLICATION || 1), retries: Number(env.L3_KAFKA_RETRIES || 2), readTimeoutMs: Number(env.L3_KAFKA_READ_MS || 15000),
      connectionTimeout: Number(env.L3_KAFKA_CONNECT_MS || 10000),
      ssl: env.L3_KAFKA_SSL === '1' ? {} : undefined,
      sasl: env.L3_KAFKA_USER ? { mechanism: env.L3_KAFKA_MECHANISM || 'scram-sha-256', username: env.L3_KAFKA_USER, password: env.L3_KAFKA_PASS || '' } : undefined });
  }
  if (kind !== 'file') throw new Error(`L3_LOG must be file or kafka, not ${kind}`);
  return createFileLog({ dir: dir || env.L3_LOG_DIR || path.join(env.L3_DIR || (env.DATA_DIR ? path.join(env.DATA_DIR, 'l3') : '.l3'), 'log'), logger, fsync: env.L3_LOG_FSYNC === '1' });
}

export const ordersTopic = (shard) => `orders.${SAFE(shard)}`;
export const votesTopic = (shard) => `votes.${SAFE(shard)}`;
