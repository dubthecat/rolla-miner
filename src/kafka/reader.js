// engine/l3/kafka/reader.js — every topic this process follows, on one connection, in one request.
//
// This is what replaces two kafkajs consumers per shard. The whole registry is polled with a single Fetch, which
// takes a list of topic-partitions, so following 79 shards costs one request and one connection rather than 158
// consumer groups. What that removes, measured on the live fleet:
//
//   subscribe        a coordinator join of ~34 s a shard (≈45 min to mine 79)   →   a Map.set, no network at all
//   consumer groups  20,230 of them against 307 topics, each one coordinated    →   none: there is no group
//   memory           ~48 MB a shard, OOM-killed every ~50 min on 1 GB           →   one fetch buffer per process
//
// The reader owns the cursor. Kafka's own offset-commit machinery is not used and could not be: a miner must
// re-read the votes topic from 0 at every start, and a committed group offset is exactly the thing that would
// quietly prevent it.
import { fetch, listOffsets, EARLIEST } from './client.js';
import { isMissingTopic } from './protocol.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * createReader({ conn, ... })
 *   subscribe(topic, fromOffset, handler) → { close() }   registers; returns at once, nothing is sent
 *   handler({ offset, value, topic, partition })          awaited, in offset order, per topic
 *
 * `missingWaitMs` is how long a topic that does not exist is left out of the fetch before it is tried again — a
 * shard whose sequencer has not booted yet, or one retired for good. It is never created: a reader that creates
 * the topic it was asked about is how a dropped shard's log kept coming back (log.js, docs/L3-MINERS.md §2).
 */
export function createReader({ conn, logger = null, maxWaitMs = 500, minBytes = 1, maxBytes = 4 << 20, partitionMaxBytes = 1 << 20, missingWaitMs = 15000, idleMs = 50, errorBackoffMs = 1000, now = Date.now } = {}) {
  const subs = new Map();            // topic → { from, next, handler, missingUntil, delivered, errors }
  const stats = { fetches: 0, records: 0, bytes: 0, errors: 0, resets: 0, missing: 0, lastError: null, maxItems: 0, lastFetchMs: 0 };
  let running = false, stopped = false, loop = null;

  function subscribe(topic, fromOffset, handler) {
    const from = Math.max(0, Number(fromOffset) || 0);
    if (subs.has(topic)) throw new Error(`kafka reader: already following ${topic}`);
    subs.set(topic, { topic, from, next: from, handler, missingUntil: 0, delivered: 0, errors: 0 });
    start();
    return {
      topic,
      close() { subs.delete(topic); },
      stats: () => { const s = subs.get(topic); return s ? { offset: s.next - 1, delivered: s.delivered, errors: s.errors, waiting: s.missingUntil > now() } : { offset: -1, delivered: 0, errors: 0, waiting: false }; },
    };
  }

  /// The partitions worth asking about this time round: everything except topics we know are not there yet,
  /// STARTING FROM A ROTATING POINT. The broker fills a response up to `maxBytes` in the order the request lists
  /// its partitions, so a fixed order starves the tail: following 20 shards from offset 0, the first benchmark had
  /// 12 of them streaming and 8 that had not delivered a single record after a minute. Rotating the start gives
  /// every partition its turn, which costs nothing and is the difference between a reader that catches up and one
  /// that catches up on the shards that happen to sort first.
  let cursor = 0;
  function items() {
    const t = now();
    const live = [];
    for (const s of subs.values()) { if (s.missingUntil > t) continue; live.push(s); }
    if (!live.length) return [];
    const start = cursor % live.length;
    cursor = (start + 1) % live.length;
    const out = new Array(live.length);
    for (let i = 0; i < live.length; i++) {
      const s = live[(start + i) % live.length];
      out[i] = { topic: s.topic, partition: 0, offset: s.next, maxBytes: partitionMaxBytes };
    }
    return out;
  }

  async function once() {
    const want = items();
    if (!want.length) { await sleep(idleMs); return; }
    stats.maxItems = Math.max(stats.maxItems, want.length);
    const t0 = now();
    const res = await fetch(conn, want, { maxWaitMs, minBytes, maxBytes, partitionMaxBytes });
    stats.fetches++; stats.lastFetchMs = now() - t0;
    const work = [];
    for (const part of res) {
      const s = subs.get(part.topic);
      if (!s) continue;                                   // unsubscribed while in flight
      if (part.errorCode !== 0) { await onPartitionError(s, part); continue; }
      s.missingUntil = 0;
      if (!part.records.length) continue;
      stats.bytes += part.bytes;
      // in offset order within a topic; topics run alongside each other, which is safe because every handler here
      // is already serialised per shard (miner.js `serial`)
      work.push((async () => {
        for (const rec of part.records) {
          if (rec.offset < s.next) continue;              // a re-fetch after a reset can overlap
          s.next = rec.offset + 1; s.delivered++; stats.records++;
          let value; try { value = JSON.parse(rec.value.toString('utf8')); } catch { continue; }
          await s.handler({ offset: rec.offset, value, topic: s.topic, partition: part.partition });
        }
      })());
    }
    if (work.length) await Promise.all(work);
  }

  async function onPartitionError(s, part) {
    s.errors++; stats.errors++;
    if (isMissingTopic(part.errorCode)) {
      // not an error: the sequencer has not created this shard's topic yet, or it was retired for good
      if (!s.missingUntil) { stats.missing++; logger && logger(`[kafka] ${s.topic} is not on the broker yet — waiting`); }
      s.missingUntil = now() + missingWaitMs;
      return;
    }
    if (part.errorCode === 1) {
      // OFFSET_OUT_OF_RANGE. Retention moves a topic's log START forward — `orders.19085` begins at 3,103 today —
      // so a reader asking from 0 is asking for records that no longer exist. Resume at the earliest the broker
      // still has, which is what a group consumer's `auto.offset.reset=earliest` would have done, and say so:
      // the gap is real and the caller's replay is incomplete through it.
      try {
        const [e] = await listOffsets(conn, [{ topic: s.topic, partition: 0 }], { timestamp: EARLIEST });
        const earliest = Number(e && e.offset);
        if (Number.isFinite(earliest) && earliest > s.next) {
          logger && logger(`[kafka] ${s.topic}: offset ${s.next} has aged out of the log (it starts at ${earliest}) — resuming there, ${earliest - s.next} record(s) are gone`);
          s.next = earliest; stats.resets++;
          return;
        }
        if (Number.isFinite(earliest) && earliest <= s.next) {   // ahead of the end: the topic was truncated or recreated
          logger && logger(`[kafka] ${s.topic}: offset ${s.next} is past the end — resuming at ${earliest}`);
          s.next = earliest; stats.resets++;
          return;
        }
      } catch (e) { logger && logger(`[kafka] ${s.topic}: cannot resolve the earliest offset (${e.message})`); }
      s.missingUntil = now() + errorBackoffMs;
      return;
    }
    stats.lastError = `${s.topic}: ${part.error}`;
    logger && logger(`[kafka] ${s.topic}: ${part.error} — retrying`);
    s.missingUntil = now() + errorBackoffMs;
  }

  function start() {
    if (running || stopped) return;
    running = true;
    loop = (async () => {
      while (!stopped) {
        try { await once(); }
        catch (e) {
          stats.errors++; stats.lastError = e.message;
          logger && logger(`[kafka] fetch failed: ${e.message}`);
          await sleep(errorBackoffMs);
        }
      }
      running = false;
    })();
  }

  async function stop() {
    stopped = true;
    try { await loop; } catch { }
    subs.clear();
  }

  return {
    subscribe, stop, start,
    get size() { return subs.size; },
    stats: () => ({ ...stats, topics: subs.size, waiting: [...subs.values()].filter((s) => s.missingUntil > now()).length }),
    cursorOf: (topic) => { const s = subs.get(topic); return s ? s.next : -1; },
  };
}
