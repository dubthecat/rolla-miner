// node --test engine/l3/kafka/kafka.test.mjs — the wire format, offline.
//
// The broker-gated half of this (set L3_KAFKA_BROKERS) proves we agree with a real Redpanda. This half proves the
// codec on its own, because an offset IS the consensus order of this venue: a decoder that is off by one, drops
// the last record of a batch, or quietly accepts a corrupt one would not look like a bug, it would look like a
// fork. So the refusals are tested as carefully as the successes.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Writer, Reader, frame, API, errName, isMissingTopic } from './protocol.js';
import { crc32c, encodeRecordSet, decodeRecordSet } from './records.js';

// ------------------------------------------------------------------------------------------------ primitives
test('ints, strings and bytes round-trip, big-endian as Kafka requires', () => {
  const w = new Writer(8);
  w.int8(-128).int16(-32768).int32(-2147483648).int64(-9007199254740993n).string('rolla').string(null).bytes(Buffer.from('abc')).bytes(null);
  const r = new Reader(w.done());
  assert.equal(r.int8(), -128);
  assert.equal(r.int16(), -32768);
  assert.equal(r.int32(), -2147483648);
  assert.equal(r.int64(), -9007199254740993n, 'an int64 past 2^53 must survive, because an offset can be one');
  assert.equal(r.string(), 'rolla');
  assert.equal(r.string(), null);
  assert.deepEqual(r.bytes(), Buffer.from('abc'));
  assert.equal(r.bytes(), null);
  assert.equal(r.left, 0, 'the reader consumed exactly what was written');
});

test('a string is counted in BYTES, not characters', () => {
  const w = new Writer(8).string('héllo €');
  const r = new Reader(w.done());
  assert.equal(r.int16(), Buffer.byteLength('héllo €', 'utf8'));
  assert.equal(new Reader(w.done()).string(), 'héllo €');
});

test('zig-zag varints round-trip across the boundaries', () => {
  for (const v of [0, -1, 1, 63, 64, -64, -65, 127, 128, 8191, 8192, 2147483647, -2147483648, 4294967296, -4294967296]) {
    const w = new Writer(16).varint(v);
    assert.equal(Number(new Reader(w.done()).varint()), v, `varint ${v}`);
  }
  // the encoding itself, so a change of mind about it fails loudly: zig-zag puts -1 at 1 and 1 at 2
  assert.deepEqual([...new Writer(4).varint(0).done()], [0]);
  assert.deepEqual([...new Writer(4).varint(-1).done()], [1]);
  assert.deepEqual([...new Writer(4).varint(1).done()], [2]);
  assert.deepEqual([...new Writer(4).varint(-2).done()], [3]);
});

test('an array writes its count first, and a null array writes -1', () => {
  const w = new Writer(16).array(['a', 'bb'], (ww, s) => ww.string(s)).array(null, () => { });
  const r = new Reader(w.done());
  assert.deepEqual(r.array((rr) => rr.string()), ['a', 'bb']);
  assert.equal(r.array((rr) => rr.string()), null);
});

test('a reader refuses to run off the end rather than invent a value', () => {
  const r = new Reader(Buffer.from([0, 1]));
  assert.throws(() => r.int32(), /truncated/);
});

test('a request frame is size, apiKey, apiVersion, correlationId, clientId, body', () => {
  const body = Buffer.from([9, 9]);
  const f = frame({ apiKey: API.Fetch, apiVersion: 11, correlationId: 7, clientId: 'c', body });
  assert.equal(f.readInt32BE(0), f.length - 4, 'the size field excludes itself');
  const r = new Reader(f, 4);
  assert.equal(r.int16(), API.Fetch);
  assert.equal(r.int16(), 11);
  assert.equal(r.int32(), 7);
  assert.equal(r.string(), 'c');
  assert.deepEqual(r.raw(2), body);
});

test('error codes are named where we know them and numbered where we do not', () => {
  assert.equal(errName(3), 'UNKNOWN_TOPIC_OR_PARTITION');
  assert.equal(errName(0), 'NONE');
  assert.equal(errName(999), 'ERROR_999');
  assert.equal(isMissingTopic(3), true);
  assert.equal(isMissingTopic(100), true);
  assert.equal(isMissingTopic(1), false, 'OFFSET_OUT_OF_RANGE is not a missing topic — it is a retention reset');
});

// ------------------------------------------------------------------------------------------------ CRC-32C
test('crc32c matches the published check value', () => {
  // the Castagnoli check value for "123456789" — NOT zlib's CRC-32, which would be 0xCBF43926
  assert.equal(crc32c(Buffer.from('123456789')).toString(16), 'e3069283');
  assert.equal(crc32c(Buffer.alloc(0)), 0);
  assert.notEqual(crc32c(Buffer.from('a')), crc32c(Buffer.from('b')));
});

// ------------------------------------------------------------------------------------------------ record sets
const vals = (n) => Array.from({ length: n }, (_, i) => Buffer.from(JSON.stringify({ i, pad: 'x'.repeat(i % 11) })));

test('a record set round-trips, with the offsets the deltas describe', () => {
  const set = encodeRecordSet(vals(5), { baseTimestamp: 1700000000000 });
  const d = decodeRecordSet(set);
  assert.equal(d.records.length, 5);
  assert.equal(d.batches, 1);
  assert.equal(d.partial, false);
  assert.deepEqual(d.records.map((r) => r.offset), [0, 1, 2, 3, 4]);
  assert.deepEqual(d.records.map((r) => JSON.parse(r.value.toString()).i), [0, 1, 2, 3, 4]);
  assert.equal(d.records[0].timestamp, 1700000000000);
  assert.equal(d.records[0].key, null);
  assert.deepEqual(d.records[0].headers, []);
});

test('several batches concatenated are one stream of records, which is what a fetch returns', () => {
  const a = encodeRecordSet(vals(3)), b = encodeRecordSet(vals(2));
  const d = decodeRecordSet(Buffer.concat([a, b]));
  assert.equal(d.batches, 2);
  assert.equal(d.records.length, 5);
  assert.equal(d.partial, false);
});

test('a cut trailing batch is normal and is dropped, not thrown over', () => {
  // the broker fills maxBytes and stops mid-batch; a reader that threw here would fail on every busy fetch
  const whole = Buffer.concat([encodeRecordSet(vals(3)), encodeRecordSet(vals(4))]);
  for (const cut of [1, 5, 20, 60]) {
    const d = decodeRecordSet(whole.subarray(0, whole.length - cut));
    assert.equal(d.partial, true, `cut ${cut}`);
    assert.equal(d.records.length, 3, `cut ${cut}: the whole first batch survives and the cut one is dropped`);
    assert.ok(d.bytes > 0 && d.bytes <= whole.length);
  }
});

test('a UTF-8 payload survives the record set byte for byte', () => {
  const v = [Buffer.from(JSON.stringify({ q: 'Will € reach ¥100? — 日本語' }))];
  const d = decodeRecordSet(encodeRecordSet(v));
  assert.equal(d.records[0].value.toString('utf8'), v[0].toString('utf8'));
  JSON.parse(d.records[0].value.toString('utf8'));
});

test('an empty batch decodes to no records, not to a throw', () => {
  const d = decodeRecordSet(encodeRecordSet([]));
  assert.deepEqual(d.records, []);
  assert.equal(d.batches, 1);
});

test('a corrupt batch is refused, never half-read', () => {
  const set = encodeRecordSet(vals(4));
  // flip a byte in the record data: the CRC must catch it
  const bad = Buffer.from(set); bad[bad.length - 5] ^= 0xff;
  assert.throws(() => decodeRecordSet(bad), /CRC mismatch/);
  // and with the check off it is still not allowed to invent records beyond the batch
  assert.doesNotThrow(() => decodeRecordSet(Buffer.from(set), { checkCrc: false }));
});

test('a pre-0.11 message set is refused by name rather than decoded as nonsense', () => {
  const set = Buffer.from(encodeRecordSet(vals(2)));
  set[16] = 1;                                   // magic, at baseOffset(8) + batchLength(4) + leaderEpoch(4)
  assert.throws(() => decodeRecordSet(set, { checkCrc: false }), /magic 1, expected 2/);
});

test('a compressed batch is refused, because this venue does not produce one', () => {
  const set = Buffer.from(encodeRecordSet(vals(2)));
  // attributes is the int16 right after the crc: 8 + 4 + 4 + 1 + 4 = 21
  set.writeInt16BE(1, 21);                       // gzip
  assert.throws(() => decodeRecordSet(set, { checkCrc: false }), /gzip-compressed/);
});

test('a control batch occupies offsets but is never handed over as a record', () => {
  // transaction markers: the venue writes none, but a reader that counted them as data would hand the miner a
  // record it cannot parse and shift every offset after it
  const set = Buffer.from(encodeRecordSet(vals(3)));
  set.writeInt16BE(0x20, 21);                    // isControl
  const d = decodeRecordSet(set, { checkCrc: false });
  assert.deepEqual(d.records, []);
  assert.equal(d.batches, 1, 'the batch is still consumed, so the cursor still advances past it');
});

test('decoding is stable over many sizes, including a batch of one', () => {
  for (const n of [1, 2, 7, 64, 200]) {
    const d = decodeRecordSet(encodeRecordSet(vals(n)));
    assert.equal(d.records.length, n, `n=${n}`);
    assert.deepEqual(d.records.map((r) => r.offset), Array.from({ length: n }, (_, i) => i));
  }
});

// ---------------------------------------------------------------------------- against a real broker
// Skipped without L3_KAFKA_BROKERS so the suite stays green offline:
//   L3_KAFKA_BROKERS=rolla-l3-broker.fly.dev:9092 node --test engine/l3/kafka/kafka.test.mjs
const BROKERS = process.env.L3_KAFKA_BROKERS || '';
const skip = !BROKERS && 'set L3_KAFKA_BROKERS to run against a broker';

test('the broker speaks the versions we pin, and says so before anything is decoded', { skip }, async () => {
  const { createConnection, apiVersions, FETCH_VERSION, METADATA_VERSION, LISTOFFSETS_VERSION } = await import('./client.js');
  const [host, port] = BROKERS.split(',')[0].split(':');
  const conn = createConnection({ host, port: Number(port), clientId: 'rolla-l3-apicheck' });
  try {
    const vs = await apiVersions(conn);
    for (const [name, key, pin] of [['Fetch', API.Fetch, FETCH_VERSION], ['Metadata', API.Metadata, METADATA_VERSION], ['ListOffsets', API.ListOffsets, LISTOFFSETS_VERSION]]) {
      const v = vs.get(key);
      assert.ok(v, `${name} is not supported at all`);
      assert.ok(pin >= v.min && pin <= v.max, `${name} v${pin} is outside the broker's ${v.min}..${v.max}`);
    }
  } finally { conn.close(); }
});

test('what we decode is what kafkajs decodes, record for record, on a real topic', { skip }, async () => {
  // The one test that matters for adopting this: the same bytes, through two independent decoders, must give the
  // same JSON at the same offsets. Written against a scratch topic so it does not depend on the venue's state.
  const { createConnection, fetch } = await import('./client.js');
  const { createKafkaLog } = await import('../miner/log.js');
  const topic = `scratch.native.${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
  const log = await createKafkaLog({ brokers: BROKERS, clientId: 'native-cmp', groupPrefix: `native-cmp-${Date.now()}`, logger: () => { } });
  const [host, port] = BROKERS.split(',')[0].split(':');
  const conn = createConnection({ host, port: Number(port), clientId: 'native-cmp' });
  try {
    await log.ensureTopic(topic);
    // a payload that exercises the awkward parts: UTF-8, a long record, an empty object
    const sent = [{ i: 0, q: 'Will € reach ¥100? — 日本語' }, { i: 1, pad: 'x'.repeat(5000) }, { i: 2 }, { i: 3, nested: { a: [1, 2, 3], b: null } }];
    for (const v of sent) await log.append(topic, v);
    const [mine] = await fetch(conn, [{ topic, partition: 0, offset: 0 }], { maxWaitMs: 4000, partitionMaxBytes: 1 << 20 });
    assert.equal(mine.errorCode, 0, `fetch said ${mine.error}`);
    assert.equal(mine.records.length, sent.length, 'a different number of records');
    assert.equal(mine.highWatermark, sent.length);
    const theirs = await log.read(topic, 0, sent.length);
    assert.equal(theirs.length, sent.length);
    for (let i = 0; i < sent.length; i++) {
      assert.equal(mine.records[i].offset, i, 'offsets must be exact: an offset IS the consensus order');
      assert.equal(mine.records[i].offset, Number(theirs[i].offset));
      assert.deepEqual(JSON.parse(mine.records[i].value.toString('utf8')), sent[i]);
      assert.deepEqual(JSON.parse(mine.records[i].value.toString('utf8')), theirs[i].value);
    }
    // and a fetch from the middle starts exactly there
    const [tail] = await fetch(conn, [{ topic, partition: 0, offset: 2 }], { maxWaitMs: 4000 });
    assert.deepEqual(tail.records.map((r) => r.offset), [2, 3]);
  } finally {
    try { await log.drop(topic); } catch { }
    conn.close(); await log.close();
  }
});

test('the reader follows several topics on one connection and delivers each in order', { skip }, async () => {
  const { createConnection } = await import('./client.js');
  const { createReader } = await import('./reader.js');
  const { createKafkaLog } = await import('../miner/log.js');
  const stamp = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
  const topics = [0, 1, 2].map((i) => `scratch.mux.${stamp}.${i}`);
  const log = await createKafkaLog({ brokers: BROKERS, clientId: 'native-mux', groupPrefix: `native-mux-${Date.now()}`, logger: () => { } });
  const [host, port] = BROKERS.split(',')[0].split(':');
  const conn = createConnection({ host, port: Number(port), clientId: 'native-mux' });
  const reader = createReader({ conn, logger: () => { }, maxWaitMs: 250 });
  try {
    for (const t of topics) { await log.ensureTopic(t); for (let i = 0; i < 5; i++) await log.append(t, { t, i }); }
    const got = new Map(topics.map((t) => [t, []]));
    const t0 = Date.now();
    for (const t of topics) reader.subscribe(t, 0, async ({ offset, value }) => { got.get(t).push([offset, value.i]); });
    assert.ok(Date.now() - t0 < 100, 'subscribing is not supposed to touch the network');
    const dl = Date.now() + 30000;
    while (Date.now() < dl && [...got.values()].some((a) => a.length < 5)) await new Promise((r) => setTimeout(r, 25));
    for (const t of topics) {
      assert.deepEqual(got.get(t), [[0, 0], [1, 1], [2, 2], [3, 3], [4, 4]], `${t} out of order or incomplete`);
    }
    const s = reader.stats();
    assert.equal(s.topics, topics.length);
    assert.ok(s.fetches >= 1);
    // one connection for all of them, and the handle's own cursor is right
    assert.equal(reader.cursorOf(topics[0]), 5);
  } finally {
    await reader.stop(); conn.close();
    for (const t of topics) { try { await log.drop(t); } catch { } }
    await log.close();
  }
});
