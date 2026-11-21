// engine/l3/kafka/protocol.js — the Kafka wire primitives this venue needs, and nothing else.
//
// Why our own, when kafkajs is right there: we do not want a consumer GROUP, and a group is most of what a Kafka
// client is. Every miner must see EVERY record — this is a replicated log, not a work queue — so the group's whole
// purpose, dividing partitions between members, is the one behaviour that must not happen here. kafkajs has no
// consumer without a group, so the code worked around it with a random group id per subscription, and the
// workaround is what cost: 20,230 consumer groups against 307 topics on the live broker, ~34 s of coordinator
// join per shard (≈45 minutes to mine 79), and two connections plus two fetch buffers per shard, which is how a
// miner reached ~48 MB a shard and was OOM-killed on a 1 GB machine.
//
// Without a group the same job is a Fetch, and Kafka's Fetch takes a LIST of topic-partitions — so every shard a
// miner follows is ONE request on ONE connection. No coordinator, no join, no rebalance, no heartbeat, no commit.
//
// This file is written from the Kafka protocol specification, not copied from kafkajs (which is MIT and archived
// at 2.2.4 either way). It is deliberately small: ints, varints, strings, bytes, and the request/response frame.
// Numbers are read as BigInt only where Kafka says int64 and the value can exceed 2^53 — offsets can, timestamps
// cannot in any run we will see, but both are kept exact because an offset IS the consensus order.

/// A growable write buffer. Kafka is big-endian everywhere.
export class Writer {
  constructor(size = 256) { this.b = Buffer.allocUnsafe(size); this.n = 0; }
  _room(k) { if (this.n + k <= this.b.length) return; let len = this.b.length * 2; while (len < this.n + k) len *= 2; const nb = Buffer.allocUnsafe(len); this.b.copy(nb, 0, 0, this.n); this.b = nb; }
  int8(v) { this._room(1); this.b.writeInt8(v, this.n); this.n += 1; return this; }
  /// unsigned, for varint continuation bytes: `b | 0x80` is past 127 and writeInt8 refuses it
  uint8(v) { this._room(1); this.b.writeUInt8(v, this.n); this.n += 1; return this; }
  int16(v) { this._room(2); this.b.writeInt16BE(v, this.n); this.n += 2; return this; }
  int32(v) { this._room(4); this.b.writeInt32BE(v, this.n); this.n += 4; return this; }
  int64(v) { this._room(8); this.b.writeBigInt64BE(BigInt(v), this.n); this.n += 8; return this; }
  /// a Kafka nullable string: int16 length (-1 = null) then UTF-8
  string(s) {
    if (s === null || s === undefined) return this.int16(-1);
    const buf = Buffer.from(String(s), 'utf8');
    this.int16(buf.length); this._room(buf.length); buf.copy(this.b, this.n); this.n += buf.length; return this;
  }
  /// a Kafka nullable bytes: int32 length (-1 = null) then the bytes
  bytes(buf) {
    if (buf === null || buf === undefined) return this.int32(-1);
    this.int32(buf.length); this._room(buf.length); buf.copy(this.b, this.n); this.n += buf.length; return this;
  }
  raw(buf) { this._room(buf.length); buf.copy(this.b, this.n); this.n += buf.length; return this; }
  /// an array of T: int32 count then each item (-1 = null)
  array(items, write) {
    if (items === null || items === undefined) return this.int32(-1);
    this.int32(items.length); for (const it of items) write(this, it); return this;
  }
  /// zig-zag varint, the record-batch encoding (Kafka calls it varint; it is Protocol Buffers' signed varint)
  varint(v) { return this.uvarint(BigInt(v) >= 0n ? BigInt(v) * 2n : BigInt(v) * -2n - 1n); }
  uvarint(v) {
    let x = BigInt(v);
    for (;;) { const b = Number(x & 0x7fn); x >>= 7n; if (x === 0n) { this.uint8(b); return this; } this.uint8(b | 0x80); }
  }
  done() { return this.b.subarray(0, this.n); }
  get length() { return this.n; }
}

/// A read cursor over a response. Every read is bounds-checked: a truncated or unexpected response must throw
/// where it happens, not produce a plausible-looking record.
export class Reader {
  constructor(buf, at = 0) { this.b = buf; this.i = at; }
  _need(k) { if (this.i + k > this.b.length) throw new Error(`kafka: response truncated (wanted ${k} at ${this.i} of ${this.b.length})`); }
  int8() { this._need(1); const v = this.b.readInt8(this.i); this.i += 1; return v; }
  int16() { this._need(2); const v = this.b.readInt16BE(this.i); this.i += 2; return v; }
  int32() { this._need(4); const v = this.b.readInt32BE(this.i); this.i += 4; return v; }
  uint32() { this._need(4); const v = this.b.readUInt32BE(this.i); this.i += 4; return v; }
  int64() { this._need(8); const v = this.b.readBigInt64BE(this.i); this.i += 8; return v; }
  string() { const n = this.int16(); if (n < 0) return null; this._need(n); const s = this.b.toString('utf8', this.i, this.i + n); this.i += n; return s; }
  bytes() { const n = this.int32(); if (n < 0) return null; this._need(n); const v = this.b.subarray(this.i, this.i + n); this.i += n; return v; }
  raw(n) { this._need(n); const v = this.b.subarray(this.i, this.i + n); this.i += n; return v; }
  array(read) { const n = this.int32(); if (n < 0) return null; const out = new Array(n); for (let k = 0; k < n; k++) out[k] = read(this); return out; }
  uvarint() {
    let shift = 0n, result = 0n;
    for (;;) { const b = this.int8() & 0xff; result |= BigInt(b & 0x7f) << shift; if ((b & 0x80) === 0) return result; shift += 7n; if (shift > 70n) throw new Error('kafka: varint too long'); }
  }
  varint() { const u = this.uvarint(); return (u >> 1n) ^ -(u & 1n); }
  get left() { return this.b.length - this.i; }
  get at() { return this.i; }
  seek(i) { this.i = i; return this; }
}

/// The API keys we speak. Everything else a Kafka client does — the group coordinator, joins, heartbeats, offset
/// commits — is exactly what this client exists not to do.
export const API = { Produce: 0, Fetch: 1, ListOffsets: 2, Metadata: 3, ApiVersions: 18, CreateTopics: 19, DeleteTopics: 20 };

/// A request frame: size, apiKey, apiVersion, correlationId, clientId, then the body.
export function frame({ apiKey, apiVersion, correlationId, clientId, body }) {
  const head = new Writer(64).int16(apiKey).int16(apiVersion).int32(correlationId).string(clientId);
  const size = head.length + body.length;
  return new Writer(size + 4).int32(size).raw(head.done()).raw(body).done();
}

/// Kafka error codes we name. The rest are reported by number, which is still actionable.
export const ERRORS = {
  '-1': 'UNKNOWN_SERVER_ERROR', 0: 'NONE', 1: 'OFFSET_OUT_OF_RANGE', 3: 'UNKNOWN_TOPIC_OR_PARTITION',
  5: 'LEADER_NOT_AVAILABLE', 6: 'NOT_LEADER_OR_FOLLOWER', 7: 'REQUEST_TIMED_OUT', 9: 'REPLICA_NOT_AVAILABLE',
  10: 'MESSAGE_TOO_LARGE', 13: 'NETWORK_EXCEPTION', 17: 'INVALID_TOPIC_EXCEPTION', 36: 'TOPIC_ALREADY_EXISTS',
  37: 'INVALID_PARTITIONS', 41: 'NOT_CONTROLLER', 56: 'KAFKA_STORAGE_ERROR', 75: 'OFFSET_NOT_AVAILABLE',
  100: 'UNKNOWN_TOPIC_ID',
};
export const errName = (code) => ERRORS[String(code)] || `ERROR_${code}`;
/// a topic that is simply not there — the one error a reader treats as "wait", not "fail" (log.js subscribe())
export const isMissingTopic = (code) => code === 3 || code === 100;
export class KafkaError extends Error {
  constructor(code, where) { super(`kafka: ${errName(code)} (${code})${where ? ` on ${where}` : ''}`); this.code = code; this.where = where || null; }
}
