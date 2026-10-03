// engine/l3/miner/verify.js — the signature verifier, and the worker pool that is the only reason a miner can
// keep up with a sequencer.
//
// docs/L3-MINERS.md §6, measured on the bench machine: one secp256k1 recovery costs 5.63 ms — 178/s per core,
// 950 matcher operations, 208 keccaks. It is the ENTIRE per-order cost of the L3; everything else in a miner is
// free. So the unit of work here is deliberately the whole expensive half of admitting an order —
//
//     { order, signature }  →  { hash, signer }        (EIP-712 digest + public-key recovery)
//
// — and it runs in a `node:worker_threads` pool, so the miner's main thread never touches a curve and its
// /healthz keeps answering while a batch is being verified. Chunks go out in order and results come back
// reassembled in order: verification is parallel, sequencing stays deterministic.
//
// Why recover and not verify: an address is a hash of a public key, so there is nothing to verify AGAINST
// without first recovering (and noble's verify-with-a-known-key measures the same 5.58 ms anyway, 5.34 ms even
// with a precomputed wNAF table for a hot account — see §6). The recovered signer is then compared with the
// order's `user`, or treated as a session key whose grant the caller checks.
//
// Two alignment rules with RollaBook, because a miner that accepts what the chain refuses is a fork waiting to
// happen:
//   * high-S signatures are REJECTED. OpenZeppelin's ECDSA.tryRecover (what RollaBook._signer uses) returns
//     InvalidSignatureS for s > n/2 and the settle reverts, so a miner must not count such an order as valid.
//   * v must be 27 or 28 (0 or 1 after normalising); anything else is not a signature the contract can use.
//
// Swapping noble for native libsecp256k1 (30–40k/s/core, a 170× improvement) is a change to `recoverSigner`
// alone — that is the whole point of this file being one file.
import { isMainThread, parentPort, workerData, Worker } from 'node:worker_threads';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { secp256k1 } from '@noble/curves/secp256k1';
import { keccak_256 } from '@noble/hashes/sha3';
import { hashTypedData } from 'viem';
import { L3_ORDER_TYPES, parseL3Order } from '../desk.js';

// Native bindings when they are built for this platform (both packages ship prebuilds and fall back to pure JS
// themselves): libsecp256k1 recovery is ~160 µs against ~5.6 ms for noble, and native keccak ~5 µs against ~25.
// The miner's budget is signatures, so this is the single largest lever below a native miner.
const require = createRequire(import.meta.url);
let KECCAK = null, SECP = null;
try { KECCAK = require('keccak'); } catch {}
try { SECP = require('secp256k1'); if (typeof SECP.ecdsaRecover !== 'function') SECP = null; } catch {}
export const NATIVE = { keccak: !!KECCAK, secp256k1: !!SECP };
// one hasher, reset per call: the binding's hash object is a Transform stream and building one per call cost
// ~12 µs of the ~13 µs a hash took; digest() already re-initializes the native state, so clearing _finalized is the whole reset
let HASHER = null;
export const keccak256 = KECCAK
  ? (b) => { if (!HASHER) HASHER = KECCAK('keccak256'); else HASHER._finalized = false; HASHER.update(Buffer.from(b.buffer, b.byteOffset, b.byteLength)); return new Uint8Array(HASHER.digest()); }   // digest() re-initializes the state itself but leaves _finalized set
  : (b) => keccak_256(b);
const SECP_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141n, SECP_HALF = SECP_N >> 1n;

const ROLE = 'l3-verify';
const HEX = '0123456789abcdef';
const hex = (b) => { let s = '0x'; for (let i = 0; i < b.length; i++) s += HEX[b[i] >> 4] + HEX[b[i] & 15]; return s; };
const bytes = (h) => { const s = h.startsWith('0x') ? h.slice(2) : h; const o = new Uint8Array(s.length >> 1); for (let i = 0; i < o.length; i++) o[i] = parseInt(s.substr(i * 2, 2), 16); return o; };

/// the EIP-712 digest of a serialized L3 order under a domain. The miner must recompute this itself: a batch
/// that merely CLAIMS an order hash proves nothing. Hand-packed abi.encode of the L3Order struct (13 words) plus the
/// cached domain separator: two keccaks instead of viem's generic typed-data path (~600 µs). verify.test.mjs pins
/// it byte for byte against hashTypedData.
const utf8 = (s) => new TextEncoder().encode(s);
const ORDER_TYPEHASH = keccak256(utf8('L3Order(address user,uint256 marketId,uint256 outcome,address token,bool buy,uint256 price,uint256 size,uint256 deadline,uint256 nonce,uint256 salt,bool postOnly,bool ioc)'));
const DOMAIN_TYPEHASH = keccak256(utf8('EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)'));
const putUint = (b, off, x) => { let v = BigInt(x); if (v < 0n) throw new Error('negative'); for (let i = 31; i >= 0; i--) { b[off + i] = Number(v & 255n); v >>= 8n; } if (v !== 0n) throw new Error('uint256 overflow'); };
const putAddr = (b, off, a) => { const h = String(a).slice(2).toLowerCase(); if (h.length !== 40) throw new Error('address'); for (let i = 0; i < 20; i++) b[off + 12 + i] = parseInt(h.substr(i * 2, 2), 16); };
const domainSeps = new Map();
export function domainSeparator(d) {
  const key = `${d.name}|${d.version}|${d.chainId}|${String(d.verifyingContract).toLowerCase()}`; const hit = domainSeps.get(key); if (hit) return hit;
  const b = new Uint8Array(160); b.set(DOMAIN_TYPEHASH, 0); b.set(keccak256(utf8(String(d.name))), 32); b.set(keccak256(utf8(String(d.version))), 64); putUint(b, 96, d.chainId); putAddr(b, 128, d.verifyingContract);
  const v = keccak256(b); domainSeps.set(key, v); return v;
}
export function orderHash(domain, order) {
  const o = parseL3Order(order);
  const b = new Uint8Array(416); b.set(ORDER_TYPEHASH, 0);
  putAddr(b, 32, o.user); putUint(b, 64, o.marketId); putUint(b, 96, o.outcome); putAddr(b, 128, o.token); b[191] = o.buy ? 1 : 0;
  putUint(b, 192, o.price); putUint(b, 224, o.size); putUint(b, 256, o.deadline); putUint(b, 288, o.nonce); putUint(b, 320, o.salt); b[383] = o.postOnly ? 1 : 0; b[415] = o.ioc ? 1 : 0;
  const d = new Uint8Array(66); d[0] = 0x19; d[1] = 0x01; d.set(domainSeparator(domain), 2); d.set(keccak256(b), 34);
  return hex(keccak256(d));
}
/// viem's generic path, the reference the fast hash is pinned against
export const orderHashViem = (domain, order) => hashTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: parseL3Order(order) });

/// recover the signer of a 65-byte (r,s,v) signature over a 32-byte digest. Lower-case address, or null when the
/// signature is malformed, high-S, or does not recover.
export function recoverSigner(hashHex, sigHex) {
  if (typeof hashHex !== 'string' || typeof sigHex !== 'string') return null;
  if (!/^0x[0-9a-fA-F]{64}$/.test(hashHex) || !/^0x[0-9a-fA-F]{130}$/.test(sigHex)) return null;
  const s = bytes(sigHex);
  let v = s[64]; if (v >= 27) v -= 27;
  if (v !== 0 && v !== 1) return null;
  // high S is malleable and the chain's ECDSA.tryRecover refuses it — so does the miner, on either path
  let sv = 0n; for (let i = 32; i < 64; i++) sv = (sv << 8n) | BigInt(s[i]); if (sv === 0n || sv > SECP_HALF) return null;
  try {
    if (SECP) { const pk = SECP.ecdsaRecover(s.subarray(0, 64), v, bytes(hashHex), false); return hex(keccak256(pk.subarray(1)).subarray(12)); }
    const sig = secp256k1.Signature.fromCompact(s.subarray(0, 64)).addRecoveryBit(v);
    const pk = sig.recoverPublicKey(hashHex.slice(2)).toRawBytes(false).subarray(1);
    return hex(keccak256(pk).subarray(12));
  } catch { return null; }
}

/// EIP-191 ("personal_sign") over a 32-byte digest: keccak256("\x19Ethereum Signed Message:\n32" ‖ digest).
/// Batch and vote signatures use this, not a bare digest signature, because it is what the venue already signs
/// everywhere else (engine/settlement.js, engine/rolla-node.mjs) and what RollaQuorumVerifier checks on chain —
/// so an on-chain miner registry can verify a vote with the code that already exists.
const EIP191 = new TextEncoder().encode('\x19Ethereum Signed Message:\n32');
export function eip191Digest(digest) {
  const d = bytes(digest);
  if (d.length !== 32) throw new Error('eip191Digest wants 32 bytes');
  const b = new Uint8Array(EIP191.length + 32); b.set(EIP191, 0); b.set(d, EIP191.length);
  return hex(keccak256(b));
}
/// recover the signer of signDigest(account, digest)
export const recoverDigestSigner = (digest, sig) => { try { return recoverSigner(eip191Digest(digest), sig); } catch { return null; } };
/// sign a 32-byte digest the way the rest of the venue does
export const signDigest = (account, digest) => account.signMessage({ message: { raw: digest } });

/// one order, in the thread that asks: { hash, signer } or null
export function verifyOrderSync(domain, item) {
  try {
    const hash = item.hash && item.trusted ? item.hash : orderHash(domain, item.order);
    const signer = recoverSigner(hash, item.signature);
    return signer ? { hash, signer } : null;
  } catch { return null; }
}

// ----------------------------------------------------------------------------------------------- the pool
/// workers: 0 runs in this thread (right for a one-core machine); default min(4, cpus-1).
/// A worker is reffed only while it has work, so an idle verifier never keeps the process alive.
export function createVerifier({ domain, workers = null, logger = null } = {}) {
  const n = workers == null ? Math.max(0, Math.min(4, (os.cpus()?.length || 1) - 1)) : Math.max(0, workers | 0);
  const stats = { calls: 0, items: 0, ms: 0, workers: n, failures: 0 };
  if (!n) {
    return {
      workers: 0, stats: () => ({ ...stats }),
      async verifyOrders(items) {
        const t0 = process.hrtime.bigint();
        const out = items.map((it) => verifyOrderSync(domain, it));
        stats.calls++; stats.items += items.length; stats.ms += Number(process.hrtime.bigint() - t0) / 1e6;
        return out;
      },
      async close() {},
    };
  }
  const pool = [];
  for (let i = 0; i < n; i++) {
    const w = new Worker(fileURLToPath(import.meta.url), { workerData: { role: ROLE, domain } });
    const slot = { w, pending: new Map(), busy: 0, next: 1 };
    w.on('message', (m) => { const r = slot.pending.get(m.id); if (!r) return; slot.pending.delete(m.id); slot.busy--; if (!slot.busy) w.unref(); r(m.out); });
    w.on('error', (e) => { stats.failures++; logger && logger(`[l3verify] worker died: ${e.message}`); for (const r of slot.pending.values()) r(null); slot.pending.clear(); slot.busy = 0; });
    w.unref();
    pool.push(slot);
  }
  const ask = (slot, items) => new Promise((resolve) => {
    const id = slot.next++;
    slot.pending.set(id, resolve);
    if (!slot.busy) slot.w.ref();
    slot.busy++;
    slot.w.postMessage({ id, items });
  });
  return {
    workers: n, stats: () => ({ ...stats }),
    /// items: [{ order (serialized), signature, hash?, trusted? }] → [{ hash, signer } | null], in order
    async verifyOrders(items) {
      if (!items.length) return [];
      const t0 = process.hrtime.bigint();
      const per = Math.ceil(items.length / pool.length);
      const jobs = [];
      for (let i = 0, k = 0; i < items.length; i += per, k++) jobs.push({ at: i, p: ask(pool[k % pool.length], items.slice(i, i + per)) });
      const out = new Array(items.length).fill(null);
      for (const j of jobs) {
        const res = await j.p;
        if (!res) { stats.failures++; continue; }                       // a dead worker: those orders verify as null
        for (let i = 0; i < res.length; i++) out[j.at + i] = res[i];
      }
      stats.calls++; stats.items += items.length; stats.ms += Number(process.hrtime.bigint() - t0) / 1e6;
      return out;
    },
    async close() { for (const s of pool) { try { await s.w.terminate(); } catch {} } pool.length = 0; },
  };
}

// --------------------------------------------------------------------------------------------- the worker
// This file IS the worker: re-entered with workerData.role, it answers batches of items and nothing else. One
// file means one place where the curve library is named, which is what makes replacing it cheap.
if (!isMainThread && workerData && workerData.role === ROLE) {
  const domain = workerData.domain;
  parentPort.on('message', ({ id, items }) => {
    const out = new Array(items.length);
    for (let i = 0; i < items.length; i++) out[i] = verifyOrderSync(domain, items[i]);
    parentPort.postMessage({ id, out });
  });
}
