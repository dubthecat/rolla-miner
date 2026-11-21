// engine/l3/kafka/records.js — Kafka's RecordBatch v2, decoded.
//
// This is the only genuinely fiddly part of talking to a broker directly, and it is worth getting exactly right:
// an offset IS the consensus order of this venue, so a decoder that is off by one, or that silently drops the last
// record of a batch, would not look like a bug — it would look like a fork.
//
// The format (Kafka ≥ 0.11, magic 2), big-endian:
//
//   baseOffset        int64        the offset of the FIRST record; each record carries a varint delta from it
//   batchLength       int32        bytes after this field
//   partitionLeaderEpoch int32
//   magic             int8         2, or we refuse: v0/v1 message sets are a different format entirely
//   crc               uint32       CRC-32C (Castagnoli) over everything after this field
//   attributes        int16        bits 0-2 compression, 3 timestampType, 4 isTransactional, 5 isControl
//   lastOffsetDelta   int32
//   firstTimestamp    int64
//   maxTimestamp      int64
//   producerId        int64
//   producerEpoch     int16
//   baseSequence      int32
//   recordCount       int32
//   records           recordCount x Record
//
//   Record: varint length, int8 attributes, varint timestampDelta, varint offsetDelta,
//           varint keyLen + key, varint valueLen + value, varint headerCount + headers
//
// A fetch response's record set is a CONCATENATION of batches and the last one may be cut off mid-way — the broker
// sends whatever fits in maxBytes. A partial trailing batch is normal and must be dropped silently, which is what
// `batchLength` is for: if fewer bytes remain than it claims, stop. Getting that wrong is how a reader either
// throws on every fetch or invents a record.
import { Reader, Writer } from './protocol.js';

/// CRC-32C (Castagnoli), the polynomial Kafka uses for v2 batches — NOT the CRC-32 of zlib. Table-driven; the
/// table is built once. We verify rather than compute, so this runs once per batch, not once per record.
const CRC32C_TABLE = (() => {
  const t = new Int32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? (c >>> 1) ^ 0x82f63b78 : c >>> 1;
    t[n] = c;
  }
  return t;
})();
export function crc32c(buf) {
  let crc = 0xffffffff;
  for (let i = 0; i < buf.length; i++) crc = (crc >>> 8) ^ CRC32C_TABLE[(crc ^ buf[i]) & 0xff];
  return (crc ^ 0xffffffff) >>> 0;
}

export const COMPRESSION = { 0: 'none', 1: 'gzip', 2: 'snappy', 3: 'lz4', 4: 'zstd' };

/**
 * decodeRecordSet(buf, { checkCrc }) → { records, batches, partial, bytes }
 *   records  [{ offset, timestamp, key, value, headers }]  value/key are Buffers or null
 *   partial  true when the tail held an incomplete batch (normal: the broker fills maxBytes and cuts)
 *
 * Throws only on a batch that is present in full and malformed — a truncated tail is not an error.
 */
export function decodeRecordSet(buf, { checkCrc = true } = {}) {
  const out = []; let batches = 0, partial = false, used = 0;
  let i = 0;
  while (i < buf.length) {
    // a batch header is 61 bytes up to and including recordCount; less than that left means a cut tail
    if (buf.length - i < 12) { partial = true; break; }
    const baseOffset = buf.readBigInt64BE(i);
    const batchLength = buf.readInt32BE(i + 8);
    if (batchLength < 0 || buf.length - i - 12 < batchLength) { partial = true; break; }
    const end = i + 12 + batchLength;
    const r = new Reader(buf, i + 12);
    r.int32();                                   // partitionLeaderEpoch
    const magic = r.int8();
    if (magic !== 2) throw new Error(`kafka: record batch magic ${magic}, expected 2 (a pre-0.11 message set)`);
    const crc = r.uint32();
    const bodyFrom = r.at;
    if (checkCrc) {
      const got = crc32c(buf.subarray(bodyFrom, end));
      if (got !== crc) throw new Error(`kafka: record batch CRC mismatch at offset ${baseOffset} (got ${got}, want ${crc})`);
    }
    const attributes = r.int16();
    const codec = COMPRESSION[attributes & 0x07];
    if (codec !== 'none') throw new Error(`kafka: record batch is ${codec}-compressed; this reader is for the venue's own uncompressed batches`);
    r.int32();                                   // lastOffsetDelta
    const firstTimestamp = r.int64();
    r.int64(); r.int64(); r.int16(); r.int32();  // maxTimestamp, producerId, producerEpoch, baseSequence
    const count = r.int32();
    const isControl = (attributes & 0x20) !== 0;
    for (let n = 0; n < count; n++) {
      const len = Number(r.varint());
      const recEnd = r.at + len;
      if (recEnd > end) throw new Error(`kafka: record ${n} of the batch at ${baseOffset} runs past the batch`);
      r.int8();                                  // record attributes, unused in v2
      const tsDelta = r.varint();
      const offDelta = r.varint();
      const keyLen = Number(r.varint());
      const key = keyLen < 0 ? null : r.raw(keyLen);
      const valLen = Number(r.varint());
      const value = valLen < 0 ? null : r.raw(valLen);
      const hCount = Number(r.varint());
      const headers = [];
      for (let h = 0; h < hCount; h++) {
        const kl = Number(r.varint());
        const hk = kl < 0 ? null : r.raw(kl).toString('utf8');
        const vl = Number(r.varint());
        headers.push({ key: hk, value: vl < 0 ? null : r.raw(vl) });
      }
      r.seek(recEnd);                            // skip anything we did not name
      // A control batch carries the transaction markers, never user data: it occupies offsets and must be counted
      // in the cursor but never handed to a caller as a record.
      if (!isControl) out.push({ offset: Number(baseOffset + offDelta), timestamp: Number(firstTimestamp + tsDelta), key, value, headers });
    }
    batches++; used = end; i = end;
  }
  return { records: out, batches, partial, bytes: used };
}

/**
 * encodeRecordSet(values, { baseTimestamp }) → Buffer
 * One batch of uncompressed v2 records, which is what this venue produces: a JSON value, no key, no headers.
 * Only used by the native producer; kafkajs's producer writes the same bytes.
 */
export function encodeRecordSet(values, { baseTimestamp = Date.now() } = {}) {
  const recs = new Writer(1024);
  for (let i = 0; i < values.length; i++) {
    const value = values[i];
    const body = new Writer(value.length + 32);
    body.int8(0);                      // attributes
    body.varint(0);                    // timestampDelta — one batch, one instant
    body.varint(i);                    // offsetDelta
    body.varint(-1);                   // null key
    body.varint(value.length); body.raw(value);
    body.varint(0);                    // no headers
    const b = body.done();
    recs.varint(b.length); recs.raw(b);
  }
  const records = recs.done();
  const after = new Writer(records.length + 64);
  after.int16(0);                      // attributes: uncompressed, create time
  after.int32(Math.max(0, values.length - 1));   // lastOffsetDelta
  after.int64(baseTimestamp); after.int64(baseTimestamp);
  after.int64(-1n); after.int16(-1); after.int32(-1);   // producerId, producerEpoch, baseSequence: not idempotent
  after.int32(values.length);
  after.raw(records);
  const body = after.done();
  const crc = crc32c(body);
  const head = new Writer(body.length + 32);
  head.int64(0n);                                        // baseOffset — the broker assigns the real one
  head.int32(4 + 1 + 4 + body.length);                   // batchLength: leaderEpoch + magic + crc + body
  head.int32(-1); head.int8(2); head.int32(crc | 0);
  head.raw(body);
  return head.done();
}
