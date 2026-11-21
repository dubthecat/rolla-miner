// node --test engine/l3/miner/eachline.test.mjs — the journal reader, which is the one place a 16 MB file is read.
//
// `fs.readFileSync(f,'utf8').split('\n')` on a journal capped at L3_JOURNAL_MAX_BYTES costs a 16 MB buffer, a
// ~32 MB V8 string, an array of tens of thousands of strings and then a filtered copy of it — about 100 MB of
// transient allocation, twice per shard. A fleet member starts ~80 shards one after another, and with 6 shards up a
// miner was holding 386 MB of a 964 MB machine and was OOM-killed before it finished. eachLine reads in chunks, so
// the cost is the chunk and not the file.
//
// The thing that can go wrong in a chunked reader is a line that straddles a boundary, so that is most of this.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { eachLine } from './miner.js';

const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'l3line-'));
const write = (body) => { const d = tmp(); const f = path.join(d, 'j.jsonl'); fs.writeFileSync(f, body); return f; };
const collect = (f, chunk) => { const out = []; eachLine(f, (l) => { out.push(l); }, chunk); return out; };

test('every line comes back, in order, whatever the chunk size', () => {
  const lines = Array.from({ length: 500 }, (_, i) => JSON.stringify({ index: i, pad: 'x'.repeat(i % 37) }));
  const f = write(lines.join('\n') + '\n');
  for (const chunk of [8, 16, 64, 997, 4096, 1 << 20]) {
    assert.deepEqual(collect(f, chunk), lines, `chunk ${chunk}`);
  }
});

test('a line straddling the chunk boundary is not cut in half', () => {
  // the boundary lands inside a line, on a newline, and right after one
  const a = 'a'.repeat(30), b = 'b'.repeat(30), c = 'c'.repeat(30);
  const f = write(`${a}\n${b}\n${c}\n`);
  for (const chunk of [1, 2, 7, 30, 31, 32, 60, 61, 62, 90, 91, 92, 93]) {
    assert.deepEqual(collect(f, chunk), [a, b, c], `chunk ${chunk}`);
  }
});

test('a file with no trailing newline still yields its last line', () => {
  const f = write('one\ntwo\nthree');
  assert.deepEqual(collect(f, 4), ['one', 'two', 'three']);
});

test('empty lines are skipped, as a journal\'s trailing newline produces', () => {
  const f = write('one\n\n\ntwo\n');
  assert.deepEqual(collect(f, 3), ['one', 'two']);
});

test('returning false stops the read, which is how a refusal stops a replay', () => {
  const f = write('one\ntwo\nthree\nfour\n');
  const seen = [];
  eachLine(f, (l) => { seen.push(l); return l === 'two' ? false : undefined; }, 4);
  assert.deepEqual(seen, ['one', 'two'], 'it read past the refusal');
});

test('a missing file is not an error: a shard with no journal starts from the log', () => {
  const seen = [];
  eachLine(path.join(tmp(), 'nope.jsonl'), (l) => seen.push(l));
  assert.deepEqual(seen, []);
});

test('an empty file yields nothing', () => {
  assert.deepEqual(collect(write(''), 16), []);
});

test('multi-byte characters are not split across a chunk boundary', () => {
  // a market question can carry any UTF-8; a chunked reader that cuts a code point in half would corrupt the JSON
  const lines = [JSON.stringify({ q: 'Will € reach ¥100? — 日本語テスト' }), JSON.stringify({ q: 'ok' })];
  const f = write(lines.join('\n') + '\n');
  for (const chunk of [1, 3, 5, 8, 13, 17, 32, 64]) {
    const got = collect(f, chunk);
    assert.deepEqual(got, lines, `chunk ${chunk}`);
    for (const g of got) JSON.parse(g);   // and it still parses
  }
});
