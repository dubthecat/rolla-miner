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

  function open(topic) {
    const key = SAFE(topic); let t = topics.get(key);
    if (!t) {
      const file = path.join(dir, `${key}.jsonl`);
      let count = 0, bytes = 0;
      try { const buf = fs.readFileSync(file); bytes = buf.length; for (let i = 0; i < buf.length; i++) if (buf[i] === 10) count++; } catch {}
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
  function makeTail(topic, fromOffset, handler) {
    const t = open(topic);
    return { topic, file: t.file, pos: 0, offset: 0, from: Math.max(0, fromOffset | 0), rest: '', busy: false, closed: false, handler, errors: 0 };
  }
  async function pump(tl) {
    if (tl.busy || tl.closed || closed) return;
    tl.busy = true;
    try {
      for (;;) {
        let stat; try { stat = fs.statSync(tl.file); } catch { break; }
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
    async appendMany(topic, msgs) { return msgs.length ? write(topic, msgs) : open(topic).count; },
    async offset(topic) { return open(topic).count; },
    async read(topic, fromOffset = 0, limit = Infinity) {
      const t = open(topic); const out = [];
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
    async subscribe(topic, fromOffset, handler) {
      const tl = makeTail(topic, fromOffset, handler);
      tails.add(tl);
      if (!timer) { timer = setInterval(() => { for (const x of tails) pump(x); }, pollMs); timer.unref?.(); }
      await pump(tl);
      return { close() { tl.closed = true; tails.delete(tl); if (!tails.size && timer) { clearInterval(timer); timer = null; } }, stats: () => ({ offset: tl.offset, errors: tl.errors }) };
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
export async function createKafkaLog({ brokers, clientId = 'rolla-l3', groupPrefix = 'rolla-l3', ssl = undefined, sasl = undefined, logger = null, partitions = 1, replication = 1, acks = -1, connectionTimeout = 10000, retries = 2 } = {}) {
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
  async function send(topic, msgs) {
    await ensure(topic);
    const r = await producer.send({ topic, acks, messages: msgs.map((m) => ({ key: String(m.shard ?? m.index ?? ''), value: enc(m) })) });
    const base = r?.[0]?.baseOffset ?? r?.[0]?.offset;
    return base != null ? Number(base) : -1;
  }

  return {
    kind: 'kafka', brokers: list,
    async append(topic, msg) { return send(topic, [msg]); },
    async appendMany(topic, msgs) { return msgs.length ? send(topic, msgs) : this.offset(topic); },
    async offset(topic) {
      await ensure(topic);
      const o = await admin.fetchTopicOffsets(topic);
      return Number(o?.[0]?.high ?? 0);
    },
    async read(topic, fromOffset = 0, limit = 500) {
      const out = [];
      await new Promise((resolve, reject) => {
        this.subscribe(topic, fromOffset, async ({ offset, value }) => { out.push({ offset, value, topic }); if (out.length >= limit) resolve(); })
          .then((sub) => { setTimeout(() => { sub.close().finally(resolve); }, 3000); }, reject);
      });
      return out.slice(0, limit);
    },
    async subscribe(topic, fromOffset, handler) {
      await ensure(topic);
      const groupId = `${groupPrefix}-${topic}-${Math.random().toString(36).slice(2, 10)}`;
      // a batch of 2,000 orders is ~1.2 MB; kafkajs's default fetch (1 MiB per partition) then carries ONE batch per
      // round trip, and a miner far from the broker pays that round trip per batch on top of its own work. Fetch
      // up to 16 MiB (8 MiB per partition, the broker's message limit) so one trip carries several batches.
      const consumer = kafka.consumer({ groupId, sessionTimeout: 30000, allowAutoTopicCreation: true, maxBytesPerPartition: 8 << 20, maxBytes: 16 << 20, maxWaitTimeInMs: 50 });
      await consumer.connect();
      await consumer.subscribe({ topic, fromBeginning: true });
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
}

/// pick an implementation from the environment: L3_LOG=file|kafka (default file), L3_LOG_DIR, L3_KAFKA_BROKERS.
export async function createLog({ env = process.env, dir = null, clientId = 'rolla-l3', logger = null } = {}) {
  const kind = (env.L3_LOG || 'file').toLowerCase();
  if (kind === 'kafka' || kind === 'redpanda') {
    return createKafkaLog({ brokers: env.L3_KAFKA_BROKERS || '127.0.0.1:9092', clientId, logger,
      replication: Number(env.L3_KAFKA_REPLICATION || 1), retries: Number(env.L3_KAFKA_RETRIES || 2),
      ssl: env.L3_KAFKA_SSL === '1' ? {} : undefined,
      sasl: env.L3_KAFKA_USER ? { mechanism: env.L3_KAFKA_MECHANISM || 'scram-sha-256', username: env.L3_KAFKA_USER, password: env.L3_KAFKA_PASS || '' } : undefined });
  }
  if (kind !== 'file') throw new Error(`L3_LOG must be file or kafka, not ${kind}`);
  return createFileLog({ dir: dir || env.L3_LOG_DIR || path.join(env.L3_DIR || (env.DATA_DIR ? path.join(env.DATA_DIR, 'l3') : '.l3'), 'log'), logger, fsync: env.L3_LOG_FSYNC === '1' });
}

export const ordersTopic = (shard) => `orders.${SAFE(shard)}`;
export const votesTopic = (shard) => `votes.${SAFE(shard)}`;
