// lib/state-file.js — the safe way to keep a module's state in a JSON file on the data volume.
//
// Why this exists: engine/volume.js wrote `/data/volume.json` with a plain `fs.writeFileSync`. A deploy
// SIGKILLs the engine, and a kill landing inside that write leaves the file truncated to zero bytes. The
// next boot's `JSON.parse` throws, the module falls back to its empty state, and the venue's **total
// volume on the home page resets to zero** — a lifetime counter appearing to go down, which is what the
// owner reported on 2026-11-06 (`/data/volume.json` was 0 bytes and `since` moved to each restart).
//
// Three rules, so a counter can only ever go forward:
//   1. Never write in place. Write a temp beside the file, fsync it, then rename over the target —
//      rename is atomic on the same filesystem, so a reader sees either the old file or the new one.
//   2. Keep the previous good copy as `<file>.bak`, and on read fall back to it when the live file is
//      missing, empty or unparseable. An empty file is NOT an empty state.
//   3. Flush on the way out. A debounced save loses its last seconds to a SIGTERM unless the handler
//      writes synchronously before the process exits.
//
// `readState(file, fallback)` → `{ data, from }` where `from` is 'file' | 'bak' | 'fallback', so a caller
// can log loudly when it had to fall back instead of silently starting again from nothing.
import fs from 'node:fs';

const tmpOf = (file) => `${file}.tmp.${process.pid}`;
const dirOf = (file) => String(file).replace(/[^/\\]*$/, '') || '.';
const bakOf = (file) => `${file}.bak`;

/// read a state file, falling back to the backup before the empty state; never throws
export function readState(file, fallback = {}) {
  for (const [path, from] of [[file, 'file'], [bakOf(file), 'bak']]) {
    try {
      const raw = fs.readFileSync(path, 'utf8');
      if (!raw || !raw.trim()) continue;                 // a zero-byte file is a truncated write, not an empty state
      const data = JSON.parse(raw);
      if (data && typeof data === 'object') return { data, from };
    } catch { /* missing or corrupt: try the next one */ }
  }
  return { data: fallback, from: 'fallback' };
}

/// write a state file atomically, keeping the previous good copy as <file>.bak; returns true when it landed
export function writeState(file, data) {
  const tmp = tmpOf(file);
  let fd = null;
  try {
    // `data` may already be serialised: several callers build the body themselves, and re-stringifying it
    // would double-encode it into a JSON string.
    const text = typeof data === 'string' ? data : JSON.stringify(data);
    try { fs.mkdirSync(dirOf(file), { recursive: true }); } catch { /* already there, or unwritable: the open below reports it */ }
    fd = fs.openSync(tmp, 'w');
    fs.writeFileSync(fd, text);
    try { fs.fsyncSync(fd); } catch { /* fsync is best effort: a rename still beats a truncated file */ }
    fs.closeSync(fd); fd = null;
    // only back up a file that is actually readable: copying a truncated one would destroy the good backup
    try { const prev = fs.readFileSync(file, 'utf8'); if (prev && prev.trim()) fs.copyFileSync(file, bakOf(file)); } catch { /* no previous file, or unreadable */ }
    fs.renameSync(tmp, file);
    return true;
  } catch {
    try { if (fd !== null) fs.closeSync(fd); } catch { }
    try { fs.unlinkSync(tmp); } catch { }
    return false;
  }
}

/// run `save` once when the process is asked to stop, before it exits (idempotent, never throws)
export function flushOnExit(save, { signals = ['SIGTERM', 'SIGINT'] } = {}) {
  let done = false;
  const run = () => { if (done) return; done = true; try { save(); } catch { } };
  for (const sig of signals) { try { process.once(sig, run); } catch { } }
  try { process.once('beforeExit', run); } catch { }
  return run;
}

/// the larger of two numbers, treating anything unusable as 0 — a lifetime counter never moves backwards
export const highest = (a, b) => {
  const x = Number(a), y = Number(b);
  return Math.max(Number.isFinite(x) ? x : 0, Number.isFinite(y) ? y : 0);
};

/// read a state file AND its backup, handing both to `merge` newest-first — for a counter that may only go
/// forward, so a truncated live file cannot subtract what the backup still remembers.
export function readStates(file, fallback = {}) {
  const out = [];
  for (const [path, from] of [[file, 'file'], [bakOf(file), 'bak']]) {
    try {
      const raw = fs.readFileSync(path, 'utf8');
      if (!raw || !raw.trim()) continue;
      const data = JSON.parse(raw);
      if (data && typeof data === 'object') out.push({ data, from });
    } catch { /* missing or corrupt */ }
  }
  return out.length ? out : [{ data: fallback, from: 'fallback' }];
}
