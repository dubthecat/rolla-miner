// engine/l3/kafka/client.js — a Kafka connection that can do the three things a replicated log needs:
// ApiVersions (what does this broker speak), Metadata (who leads this partition), and Fetch (give me records from
// offset N — for MANY partitions at once).
//
// There is no group here, on purpose. See protocol.js for why: a group divides partitions between members, and
// every miner must see every record. Without a group there is no coordinator to find, no join to wait for, no
// rebalance when a shard appears, no heartbeat to keep alive and no offset to commit — and `subscribe` stops being
// a network operation at all.
//
// Protocol versions are pinned to the newest NON-FLEXIBLE version of each API (KIP-482's flexible encoding, with
// compact strings and tagged fields, starts at Fetch v12 and Metadata v9). Pinning below it keeps this file small
// and costs nothing: v11 Fetch has everything a reader wants. ApiVersions is still asked at connect, so a broker
// that cannot meet the pin says so at startup instead of returning something that decodes as nonsense.
import net from 'node:net';
import { Writer, Reader, API, frame, KafkaError, errName } from './protocol.js';
import { decodeRecordSet } from './records.js';

export const FETCH_VERSION = 11;
export const METADATA_VERSION = 4;

/// One TCP connection to one broker, with Kafka's length-prefixed framing and correlation ids.
export function createConnection({ host, port, clientId = 'rolla-l3', connectTimeoutMs = 10000, requestTimeoutMs = 30000, logger = null } = {}) {
  let sock = null, buf = Buffer.alloc(0), corr = 0, closed = false, connecting = null;
  const waiting = new Map();                       // correlationId → { resolve, reject, timer, api }

  const failAll = (err) => {
    for (const [, w] of waiting) { clearTimeout(w.timer); w.reject(err); }
    waiting.clear();
  };

  function onData(chunk) {
    buf = buf.length ? Buffer.concat([buf, chunk]) : chunk;
    for (;;) {
      if (buf.length < 4) return;
      const size = buf.readInt32BE(0);
      if (size < 4 || size > (256 << 20)) { failAll(new Error(`kafka: absurd response frame of ${size} bytes`)); return void close(); }
      if (buf.length < 4 + size) return;
      const body = buf.subarray(4, 4 + size);
      buf = buf.subarray(4 + size);
      const id = body.readInt32BE(0);
      const w = waiting.get(id);
      if (!w) { logger && logger(`[kafka] a response for correlation ${id} that nobody is waiting for`); continue; }
      waiting.delete(id); clearTimeout(w.timer);
      w.resolve(new Reader(body, 4));              // past the correlation id
    }
  }

  async function connect() {
    if (sock && !sock.destroyed) return;
    if (connecting) return connecting;
    connecting = new Promise((resolve, reject) => {
      const s = net.createConnection({ host, port });
      s.setNoDelay(true);
      const timer = setTimeout(() => { s.destroy(new Error(`kafka: connecting to ${host}:${port} timed out`)); }, connectTimeoutMs);
      s.once('connect', () => { clearTimeout(timer); sock = s; buf = Buffer.alloc(0); resolve(); });
      s.once('error', (e) => { clearTimeout(timer); if (sock === s) sock = null; failAll(e); reject(e); });
      s.once('close', () => { if (sock === s) sock = null; failAll(new Error(`kafka: ${host}:${port} closed the connection`)); });
      s.on('data', onData);
    }).finally(() => { connecting = null; });
    return connecting;
  }

  /// send one request and resolve with a Reader positioned at its body
  async function send(apiKey, apiVersion, body) {
    if (closed) throw new Error('kafka: client is closed');
    await connect();
    const id = (corr = (corr + 1) & 0x7fffffff);
    const p = new Promise((resolve, reject) => {
      const timer = setTimeout(() => { waiting.delete(id); reject(new Error(`kafka: ${host}:${port} did not answer api ${apiKey} in ${requestTimeoutMs} ms`)); }, requestTimeoutMs);
      timer.unref?.();
      waiting.set(id, { resolve, reject, timer, api: apiKey });
    });
    sock.write(frame({ apiKey, apiVersion, correlationId: id, clientId, body }));
    return p;
  }

  function close() { closed = true; failAll(new Error('kafka: client closed')); if (sock) { try { sock.destroy(); } catch { } sock = null; } }

  return {
    host, port, send, connect, close,
    get connected() { return !!sock && !sock.destroyed; },
    get pending() { return waiting.size; },
  };
}

/// ApiVersions v0: the broker's menu. Asked once per connection so a pin that the broker cannot meet is a clear
/// error at startup rather than a decode failure later.
export async function apiVersions(conn) {
  const r = await conn.send(API.ApiVersions, 0, Buffer.alloc(0));
  const err = r.int16();
  if (err !== 0) throw new KafkaError(err, 'ApiVersions');
  const out = new Map();
  const n = r.int32();
  for (let i = 0; i < n; i++) { const key = r.int16(), min = r.int16(), max = r.int16(); out.set(key, { min, max }); }
  return out;
}

/// Metadata v4: brokers, and the leader of every partition of the topics asked for. `topics: null` asks for all.
export async function metadata(conn, topics = null, { allowAutoTopicCreation = false } = {}) {
  const w = new Writer(256);
  w.array(topics, (ww, t) => ww.string(t));
  w.int8(allowAutoTopicCreation ? 1 : 0);
  const r = await conn.send(API.Metadata, METADATA_VERSION, w.done());
  r.int32();                                        // throttle_time_ms
  const brokers = r.array((rr) => ({ nodeId: rr.int32(), host: rr.string(), port: rr.int32(), rack: rr.string() }));
  r.string();                                       // cluster_id
  r.int32();                                        // controller_id
  const ts = r.array((rr) => ({
    errorCode: rr.int16(), name: rr.string(), isInternal: rr.int8() !== 0,
    partitions: rr.array((pp) => ({
      errorCode: pp.int16(), partition: pp.int32(), leader: pp.int32(),
      replicas: pp.array((x) => x.int32()), isr: pp.array((x) => x.int32()),
    })),
  }));
  return { brokers, topics: ts };
}

/**
 * Fetch v11 for MANY partitions in one request — the whole point of this client.
 *   items  [{ topic, partition, offset, maxBytes }]
 * → [{ topic, partition, errorCode, highWatermark, records, partial, bytes }]
 *
 * `maxWaitMs` is a long poll: the broker holds the request open until `minBytes` are available or the wait
 * expires, so an idle venue costs one outstanding request per broker and no polling at all.
 */
export async function fetch(conn, items, { maxWaitMs = 500, minBytes = 1, maxBytes = 4 << 20, partitionMaxBytes = 1 << 20, isolationLevel = 0 } = {}) {
  const byTopic = new Map();
  for (const it of items) { if (!byTopic.has(it.topic)) byTopic.set(it.topic, []); byTopic.get(it.topic).push(it); }
  const w = new Writer(256 + items.length * 48);
  w.int32(-1);                       // replica_id: a client, not a follower
  w.int32(maxWaitMs); w.int32(minBytes); w.int32(maxBytes);
  w.int8(isolationLevel);            // 0 = read_uncommitted; this venue writes no transactions
  w.int32(0); w.int32(0);            // session_id, session_epoch: no incremental fetch session
  w.array([...byTopic.entries()], (ww, [topic, parts]) => {
    ww.string(topic);
    ww.array(parts, (pp, it) => {
      pp.int32(it.partition ?? 0);
      pp.int32(-1);                  // current_leader_epoch: unknown, do not validate
      pp.int64(it.offset);
      pp.int64(-1);                  // log_start_offset: a client sends -1
      pp.int32(it.maxBytes ?? partitionMaxBytes);
    });
  });
  w.array([], () => { });            // forgotten_topics
  w.string('');                      // rack_id
  const r = await conn.send(API.Fetch, FETCH_VERSION, w.done());
  r.int32();                         // throttle_time_ms
  const topErr = r.int16();
  if (topErr !== 0) throw new KafkaError(topErr, 'Fetch');
  r.int32();                         // session_id
  const out = [];
  const nt = r.int32();
  for (let i = 0; i < nt; i++) {
    const topic = r.string();
    const np = r.int32();
    for (let k = 0; k < np; k++) {
      const partition = r.int32();
      const errorCode = r.int16();
      const highWatermark = Number(r.int64());
      r.int64();                     // last_stable_offset
      const logStart = Number(r.int64());
      r.array((a) => ({ producerId: a.int64(), firstOffset: a.int64() }));   // aborted_transactions
      r.int32();                     // preferred_read_replica
      const raw = r.bytes();
      let records = [], partial = false, bytes = 0;
      if (errorCode === 0 && raw && raw.length) ({ records, partial, bytes } = decodeRecordSet(raw));
      out.push({ topic, partition, errorCode, error: errorCode ? errName(errorCode) : null, highWatermark, logStart, records, partial, bytes: raw ? raw.length : 0, decoded: bytes });
    }
  }
  return out;
}

export const LISTOFFSETS_VERSION = 5;    // v6 is flexible
export const EARLIEST = -2n, LATEST = -1n;

/**
 * ListOffsets v5: the earliest or latest offset of each partition.
 *   items  [{ topic, partition, timestamp }]   timestamp EARLIEST (-2) or LATEST (-1)
 * → [{ topic, partition, errorCode, offset }]
 *
 * Needed for one case that is not optional: a topic's retention moves its log START past the offset a reader
 * asks for, and the broker answers OFFSET_OUT_OF_RANGE rather than guessing. `orders.19085` answers that for
 * offset 0 today — its first 17,000 offsets have aged out — and a reader that treated it as a hard error would
 * refuse to follow any shard older than the retention window. The high watermark comes free with every Fetch, so
 * this is the only thing ListOffsets is here for.
 */
export async function listOffsets(conn, items, { timestamp = EARLIEST, isolationLevel = 0 } = {}) {
  const byTopic = new Map();
  for (const it of items) { if (!byTopic.has(it.topic)) byTopic.set(it.topic, []); byTopic.get(it.topic).push(it); }
  const w = new Writer(128 + items.length * 24);
  w.int32(-1);                       // replica_id
  w.int8(isolationLevel);
  w.array([...byTopic.entries()], (ww, [topic, parts]) => {
    ww.string(topic);
    ww.array(parts, (pp, it) => { pp.int32(it.partition ?? 0); pp.int32(-1); pp.int64(it.timestamp ?? timestamp); });
  });
  const r = await conn.send(API.ListOffsets, LISTOFFSETS_VERSION, w.done());
  r.int32();                         // throttle_time_ms
  const out = [];
  const nt = r.int32();
  for (let i = 0; i < nt; i++) {
    const topic = r.string();
    const np = r.int32();
    for (let k = 0; k < np; k++) {
      const partition = r.int32(), errorCode = r.int16();
      r.int64();                     // timestamp
      const offset = Number(r.int64());
      r.int32();                     // leader_epoch
      out.push({ topic, partition, errorCode, error: errorCode ? errName(errorCode) : null, offset });
    }
  }
  return out;
}
