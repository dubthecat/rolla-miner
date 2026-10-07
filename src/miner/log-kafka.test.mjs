// node --test engine/l3/miner/log-kafka.test.mjs — KafkaLog.read() against a REAL broker (the Fly Redpanda,
// rolla-l3-broker.fly.dev:9092, or any L3_KAFKA_BROKERS). Skipped without L3_KAFKA_BROKERS, so the suite stays
// green offline. What it pins: a scratch topic gets three records; read(topic, 0) returns all three in order,
// read(topic, 1) the last two, read(topic, n-1, 1) exactly the tail record (what the sequencer's resume() relies
// on to continue its chain instead of forking it), read(topic, n) nothing, and offset() == 3 — each within a
// bounded time against a broker that delays a new consumer group's first rebalance (group.initial.rebalance.delay).
//   L3_KAFKA_BROKERS=rolla-l3-broker.fly.dev:9092 node --test engine/l3/miner/log-kafka.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createKafkaLog } from './log.js';

const BROKERS = process.env.L3_KAFKA_BROKERS || '';

test('KafkaLog.read: offsets 0, 1 and the tail of a scratch topic on the real broker', { skip: !BROKERS && 'set L3_KAFKA_BROKERS to run against a broker' }, async () => {
  const log = await createKafkaLog({ brokers: BROKERS, clientId: 'rolla-l3-readcheck', groupPrefix: 'rolla-l3-readcheck', logger: () => {}, connectionTimeout: 10000, retries: 2 });
  const topic = `scratch.readcheck.${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
  try {
    const recs = [{ shard: 'rc', index: 0, v: 'a' }, { shard: 'rc', index: 1, v: 'b' }, { shard: 'rc', index: 2, v: 'c' }];
    const base = await log.append(topic, recs[0]);
    assert.equal(base, 0, 'the first append lands at offset 0');
    const base2 = await log.appendMany(topic, recs.slice(1));
    assert.equal(base2, 1, 'appendMany returns the FIRST offset of the batch');
    assert.equal(await log.offset(topic), 3, 'offset() is the next offset');

    const timed = async (what, fn) => { const t = Date.now(); const r = await fn(); const ms = Date.now() - t; assert.ok(ms < 20000, `${what} took ${ms} ms`); return { r, ms }; };
    const all = await timed('read from 0', () => log.read(topic, 0, 10));
    assert.deepEqual(all.r.map((x) => x.offset), [0, 1, 2], `read(0) → ${JSON.stringify(all.r.map((x) => x.offset))}`);
    assert.deepEqual(all.r.map((x) => x.value.v), ['a', 'b', 'c']);
    const from1 = await timed('read from 1', () => log.read(topic, 1, 10));
    assert.deepEqual(from1.r.map((x) => x.offset), [1, 2], `read(1) → ${JSON.stringify(from1.r.map((x) => x.offset))}`);
    const n = await log.offset(topic);
    const tail = await timed('read the tail', () => log.read(topic, Math.max(0, n - 1), 1));
    assert.equal(tail.r.length, 1, `read(n-1, 1) → ${tail.r.length} record(s)`);
    assert.equal(tail.r[0].offset, 2); assert.equal(tail.r[0].value.v, 'c');
    const past = await timed('read past the end', () => log.read(topic, n, 5));
    assert.deepEqual(past.r, [], 'nothing past the high watermark');
    const limited = await timed('read with a limit', () => log.read(topic, 0, 2));
    assert.deepEqual(limited.r.map((x) => x.offset), [0, 1], 'the limit is honoured');
    console.log(`  read(0) ${all.ms} ms · read(1) ${from1.ms} ms · tail ${tail.ms} ms · past-end ${past.ms} ms · limit ${limited.ms} ms`);
  } finally {
    try { await log.drop?.(topic); } catch {}
    await log.close();
  }
});
