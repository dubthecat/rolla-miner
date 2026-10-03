// node --test engine/l3/miner/verify.test.mjs — the fast order hash is byte-identical to viem's typed-data hash,
// native recovery agrees with the signer, high-S is refused, and the per-order cost is printed
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';
import { orderHash, orderHashViem, recoverSigner, NATIVE } from './verify.js';
import { BOOK_DOMAIN, l3OrderFor, serializeL3Order, signL3OrderWithAccount } from '../desk.js';

const domain = BOOK_DOMAIN(46630, '0x7197A5160562516F6f8C4503dF03CD836a524D66');
const rnd = (n) => BigInt(Math.floor(Math.random() * n));
test('orderHash equals hashTypedData for 300 random orders (edge values included)', () => {
  const acct = privateKeyToAccount(generatePrivateKey());
  for (let i = 0; i < 300; i++) {
    const o = l3OrderFor({ user: acct.address, marketId: rnd(1e6), outcome: rnd(4), token: acct.address, buy: i % 2 === 0, price: 1n + rnd(1e18), size: 1n + rnd(1e24), postOnly: i % 3 === 0, ioc: i % 5 === 0 });
    if (i === 0) { o.price = (1n << 256n) - 1n; o.size = 0n; o.nonce = 0n; o.salt = (1n << 255n); }
    const ser = serializeL3Order(o); assert.equal(orderHash(domain, ser), orderHashViem(domain, ser));
  }
});
test('recoverSigner recovers the signing account and refuses a tampered signature', async () => {
  const acct = privateKeyToAccount(generatePrivateKey());
  const o = l3OrderFor({ user: acct.address, marketId: 7, outcome: 0, token: acct.address, buy: true, price: 5n * 10n ** 17n, size: 10n ** 18n });
  const sig = await signL3OrderWithAccount(acct, domain, o); const h = orderHash(domain, serializeL3Order(o));
  assert.equal(recoverSigner(h, sig), acct.address.toLowerCase());
  const bad = sig.slice(0, 10) + (sig[10] === 'a' ? 'b' : 'a') + sig.slice(11); assert.notEqual(recoverSigner(h, bad), acct.address.toLowerCase());
  // high S: flip s to n - s (same curve point, malleable) → refused like the chain does
  const n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141n; const sv = BigInt('0x' + sig.slice(66, 130)); const hs = (n - sv).toString(16).padStart(64, '0');
  const v = parseInt(sig.slice(130, 132), 16); const highS = sig.slice(0, 66) + hs + (v === 27 ? '1c' : '1b'); assert.equal(recoverSigner(h, highS), null);
});
test('cost per order (hash + recover) on this machine', async () => {
  const acct = privateKeyToAccount(generatePrivateKey()); const items = [];
  for (let i = 0; i < 200; i++) { const o = l3OrderFor({ user: acct.address, marketId: i, outcome: 0, token: acct.address, buy: true, price: 5n * 10n ** 17n, size: 10n ** 18n }); items.push({ ser: serializeL3Order(o), sig: await signL3OrderWithAccount(acct, domain, o) }); }
  const t0 = process.hrtime.bigint(); let ok = 0; for (const it of items) if (recoverSigner(orderHash(domain, it.ser), it.sig) === acct.address.toLowerCase()) ok++;
  const us = Number(process.hrtime.bigint() - t0) / 1e3 / items.length;
  console.log(`  verify: ${us.toFixed(0)} µs per order (hash + recover) → ${Math.round(1e6 / us).toLocaleString()}/s per core · native keccak ${NATIVE.keccak} · native secp256k1 ${NATIVE.secp256k1}`);
  assert.equal(ok, items.length);
});
