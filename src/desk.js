// lib/desk.js — the non-custodial one-click desk (RollaDesk.sol), shared by the browser and the engine.
//
// How the OrderPanel should use it (the Polymarket shape: funds in a contract only the user controls,
// the venue only relays signed orders):
//   1. Fund once (two wallet prompts, once):  rUSD.approve(desk, amount) → desk.deposit(amount)
//      (an email / Crossmint wallet sends the same two transactions through its own signer).
//   2. Session once (one signature, no gas):  `makeSessionKey()` → keep it in localStorage (`saveSessionKey`),
//      build `grantFor({ user, sessionKey, hours, maxNotional })`, have the WALLET sign it as EIP-712
//      (wagmi `signTypedDataAsync({ domain: DESK_DOMAIN(chainId, desk), types: GRANT_TYPES, primaryType: 'SessionGrant', message })`;
//      Crossmint email wallets: `wallet.signTypedData({ domain, types, primaryType, message, chain })` — the
//      @crossmint/wallets-sdk EVMWallet exposes signTypedData, same shape), then POST /v1/predict/desk/grant
//      { grant, signature } — the engine relays it on-chain. Wallets without typed-data signing call
//      desk.grantDirect(sessionKey, expiry, maxNotional) as a transaction instead.
//   3. Every order, zero prompts:  `orderFor({...})` → `signOrderWithSession(sessionPk, domain, order)` →
//      POST /v1/predict/desk/order { order: serializeOrder(order), signature }. The engine checks the
//      signature and the grant, executes on-chain from the operator wallet and returns the fill.
//      The contract enforces the user's `limit` (minimum out) and the session cap; it never lets the
//      operator move funds without a valid signature.
//   4. Withdraw any time, no one in the way:  desk.withdrawAll() / withdrawOutcome(...) from the wallet.
//   5. After resolution the engine sweeps: desk.redeem(user, marketId) credits winnings to the balance.
import { privateKeyToAccount, generatePrivateKey } from 'viem/accounts';

export const DESK_ABI = [
  { type: 'function', name: 'deposit', stateMutability: 'nonpayable', inputs: [{ type: 'uint256' }], outputs: [] },
  { type: 'function', name: 'withdraw', stateMutability: 'nonpayable', inputs: [{ type: 'uint256' }], outputs: [] },
  { type: 'function', name: 'withdrawTo', stateMutability: 'nonpayable', inputs: [{ type: 'uint256' }, { type: 'address' }], outputs: [] },
  { type: 'function', name: 'withdrawAll', stateMutability: 'nonpayable', inputs: [], outputs: [] },
  { type: 'function', name: 'withdrawOutcome', stateMutability: 'nonpayable', inputs: [{ type: 'uint256' }, { type: 'uint256' }, { type: 'uint256' }, { type: 'address' }], outputs: [] },
  { type: 'function', name: 'depositOutcome', stateMutability: 'nonpayable', inputs: [{ type: 'uint256' }, { type: 'uint256' }, { type: 'uint256' }], outputs: [] },
  { type: 'function', name: 'grantDirect', stateMutability: 'nonpayable', inputs: [{ type: 'address' }, { type: 'uint256' }, { type: 'uint256' }], outputs: [] },
  { type: 'function', name: 'revoke', stateMutability: 'nonpayable', inputs: [{ type: 'address' }], outputs: [] },
  { type: 'function', name: 'trade', stateMutability: 'nonpayable', inputs: [{ type: 'uint256' }, { type: 'uint256' }, { type: 'bool' }, { type: 'uint256' }, { type: 'uint256' }], outputs: [{ type: 'uint256' }, { type: 'uint256' }] },
  { type: 'function', name: 'execute', stateMutability: 'nonpayable', inputs: [{ name: 'o', type: 'tuple', components: [{ name: 'user', type: 'address' }, { name: 'marketId', type: 'uint256' }, { name: 'outcome', type: 'uint256' }, { name: 'pool', type: 'address' }, { name: 'buy', type: 'bool' }, { name: 'amount', type: 'uint256' }, { name: 'limit', type: 'uint256' }, { name: 'deadline', type: 'uint256' }, { name: 'nonce', type: 'uint256' }] }, { name: 'sig', type: 'bytes' }], outputs: [{ type: 'uint256' }, { type: 'uint256' }] },
  { type: 'function', name: 'grant', stateMutability: 'nonpayable', inputs: [{ name: 'g', type: 'tuple', components: [{ name: 'user', type: 'address' }, { name: 'sessionKey', type: 'address' }, { name: 'expiry', type: 'uint256' }, { name: 'maxNotional', type: 'uint256' }, { name: 'nonce', type: 'uint256' }] }, { name: 'sig', type: 'bytes' }], outputs: [] },
  { type: 'function', name: 'redeem', stateMutability: 'nonpayable', inputs: [{ type: 'address' }, { type: 'uint256' }], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'setBook', stateMutability: 'nonpayable', inputs: [{ type: 'uint256' }, { type: 'uint256' }, { type: 'address' }], outputs: [] },
  { type: 'function', name: 'balance', stateMutability: 'view', inputs: [{ type: 'address' }], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'holdings', stateMutability: 'view', inputs: [{ type: 'address' }, { type: 'address' }], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'holdingsOf', stateMutability: 'view', inputs: [{ type: 'address' }, { type: 'uint256' }], outputs: [{ type: 'uint256[]' }] },
  { type: 'function', name: 'grantOf', stateMutability: 'view', inputs: [{ type: 'address' }, { type: 'address' }], outputs: [{ type: 'tuple', components: [{ name: 'expiry', type: 'uint256' }, { name: 'maxNotional', type: 'uint256' }, { name: 'used', type: 'uint256' }] }] },
  { type: 'function', name: 'bookOf', stateMutability: 'view', inputs: [{ type: 'uint256' }, { type: 'uint256' }], outputs: [{ type: 'tuple', components: [{ name: 'pool', type: 'address' }, { name: 'token', type: 'address' }] }] },
  { type: 'function', name: 'usedNonce', stateMutability: 'view', inputs: [{ type: 'address' }, { type: 'uint256' }], outputs: [{ type: 'bool' }] },
  { type: 'function', name: 'operator', stateMutability: 'view', inputs: [], outputs: [{ type: 'address' }] },
  { type: 'function', name: 'owner', stateMutability: 'view', inputs: [], outputs: [{ type: 'address' }] },
  { type: 'function', name: 'hashOrder', stateMutability: 'view', inputs: [{ name: 'o', type: 'tuple', components: [{ name: 'user', type: 'address' }, { name: 'marketId', type: 'uint256' }, { name: 'outcome', type: 'uint256' }, { name: 'pool', type: 'address' }, { name: 'buy', type: 'bool' }, { name: 'amount', type: 'uint256' }, { name: 'limit', type: 'uint256' }, { name: 'deadline', type: 'uint256' }, { name: 'nonce', type: 'uint256' }] }], outputs: [{ type: 'bytes32' }] },
  { type: 'event', name: 'Executed', inputs: [{ name: 'user', type: 'address', indexed: true }, { name: 'marketId', type: 'uint256', indexed: true }, { name: 'outcome', type: 'uint256', indexed: false }, { name: 'buy', type: 'bool', indexed: false }, { name: 'amountIn', type: 'uint256', indexed: false }, { name: 'amountOut', type: 'uint256', indexed: false }, { name: 'signer', type: 'address', indexed: false }, { name: 'nonce', type: 'uint256', indexed: false }] },
  { type: 'event', name: 'Redeemed', inputs: [{ name: 'user', type: 'address', indexed: true }, { name: 'marketId', type: 'uint256', indexed: true }, { name: 'payout', type: 'uint256', indexed: false }] },
  { type: 'event', name: 'Granted', inputs: [{ name: 'user', type: 'address', indexed: true }, { name: 'sessionKey', type: 'address', indexed: true }, { name: 'expiry', type: 'uint256', indexed: false }, { name: 'maxNotional', type: 'uint256', indexed: false }] },
];

export const ORDER_TYPES = {
  Order: [
    { name: 'user', type: 'address' }, { name: 'marketId', type: 'uint256' }, { name: 'outcome', type: 'uint256' }, { name: 'pool', type: 'address' },
    { name: 'buy', type: 'bool' }, { name: 'amount', type: 'uint256' }, { name: 'limit', type: 'uint256' }, { name: 'deadline', type: 'uint256' }, { name: 'nonce', type: 'uint256' },
  ],
};
export const GRANT_TYPES = {
  SessionGrant: [
    { name: 'user', type: 'address' }, { name: 'sessionKey', type: 'address' }, { name: 'expiry', type: 'uint256' }, { name: 'maxNotional', type: 'uint256' }, { name: 'nonce', type: 'uint256' },
  ],
};
export const DESK_DOMAIN = (chainId, verifyingContract) => ({ name: 'RollaDesk', version: '1', chainId: Number(chainId), verifyingContract });

/// unordered nonces: time-based with a random tail, so two orders in the same second never collide
export function newNonce() { return BigInt(Date.now()) * 1000000n + BigInt(Math.floor(Math.random() * 1e6)); }

// ---- browser session key (kept in localStorage; the on-chain grant is what gives it power) ----
const SESSION_KEY = 'rm_session';
export function makeSessionKey() { const pk = generatePrivateKey(); return { pk, address: privateKeyToAccount(pk).address }; }
export function loadSessionKey(storage = typeof localStorage !== 'undefined' ? localStorage : null) {
  try { const pk = storage && storage.getItem(SESSION_KEY); if (!pk) return null; return { pk, address: privateKeyToAccount(pk).address }; } catch { return null; }
}
export function saveSessionKey(pk, storage = typeof localStorage !== 'undefined' ? localStorage : null) { try { storage && storage.setItem(SESSION_KEY, pk); } catch {} }
export function clearSessionKey(storage = typeof localStorage !== 'undefined' ? localStorage : null) { try { storage && storage.removeItem(SESSION_KEY); } catch {} }

/// a grant message for the wallet to sign (hours of validity, cap in collateral wei)
export function grantFor({ user, sessionKey, hours = 24, maxNotional }) {
  return { user, sessionKey, expiry: BigInt(Math.floor(Date.now() / 1000) + Math.round(hours * 3600)), maxNotional: BigInt(maxNotional), nonce: newNonce() };
}
/// an order message (amounts in wei as bigint; `limit` = minimum out the user accepts)
export function orderFor({ user, marketId, outcome = 0, pool, buy, amount, limit = 0n, ttlSec = 180 }) {
  return { user, marketId: BigInt(marketId), outcome: BigInt(outcome), pool, buy: !!buy, amount: BigInt(amount), limit: BigInt(limit), deadline: BigInt(Math.floor(Date.now() / 1000) + ttlSec), nonce: newNonce() };
}
/// sign an order with the browser session key: no wallet prompt
export async function signOrderWithSession(sessionPk, domain, order) {
  return privateKeyToAccount(sessionPk).signTypedData({ domain, types: ORDER_TYPES, primaryType: 'Order', message: order });
}
export async function signGrantWithAccount(account, domain, grant) {
  return account.signTypedData({ domain, types: GRANT_TYPES, primaryType: 'SessionGrant', message: grant });
}
// ---- JSON transport (bigints as decimal strings) ----
export const serializeOrder = (o) => ({ user: o.user, marketId: String(o.marketId), outcome: String(o.outcome), pool: o.pool, buy: !!o.buy, amount: String(o.amount), limit: String(o.limit), deadline: String(o.deadline), nonce: String(o.nonce) });
export const parseOrder = (j) => ({ user: j.user, marketId: BigInt(j.marketId), outcome: BigInt(j.outcome || 0), pool: j.pool, buy: !!j.buy, amount: BigInt(j.amount), limit: BigInt(j.limit || 0), deadline: BigInt(j.deadline), nonce: BigInt(j.nonce) });
export const serializeGrant = (g) => ({ user: g.user, sessionKey: g.sessionKey, expiry: String(g.expiry), maxNotional: String(g.maxNotional), nonce: String(g.nonce) });
export const parseGrant = (j) => ({ user: j.user, sessionKey: j.sessionKey, expiry: BigInt(j.expiry), maxNotional: BigInt(j.maxNotional), nonce: BigInt(j.nonce) });

// ---- L3 matched order book (RollaBook.sol) -------------------------------------------------------
// Limit orders are signed off chain (by the browser session key, no prompt), matched by the engine's
// book (price-time), and the fills are settled on chain in batches: RollaBook re-checks both signatures,
// both limits, cumulative size, deadline, nonce floor and cancels, then nets the batch per (user, token)
// and moves collateral / outcome tokens between RollaDeskV2 balances. Funds never leave the desk.
//   price  collateral per share, 1e18-scaled (0.52e18 = 52¢);  size  outcome tokens (1e18 units)
//   nonce  creation time in ms (bumpNonce() voids everything older);  salt  random, makes the hash unique
export const L3_ORDER_TYPES = {
  L3Order: [
    { name: 'user', type: 'address' }, { name: 'marketId', type: 'uint256' }, { name: 'outcome', type: 'uint256' }, { name: 'token', type: 'address' },
    { name: 'buy', type: 'bool' }, { name: 'price', type: 'uint256' }, { name: 'size', type: 'uint256' }, { name: 'deadline', type: 'uint256' },
    { name: 'nonce', type: 'uint256' }, { name: 'salt', type: 'uint256' }, { name: 'postOnly', type: 'bool' }, { name: 'ioc', type: 'bool' },
  ],
};
export const BOOK_DOMAIN = (chainId, verifyingContract) => ({ name: 'RollaBook', version: '1', chainId: Number(chainId), verifyingContract });
const L3_ORDER_TUPLE = { type: 'tuple', components: [
  { name: 'user', type: 'address' }, { name: 'marketId', type: 'uint256' }, { name: 'outcome', type: 'uint256' }, { name: 'token', type: 'address' },
  { name: 'buy', type: 'bool' }, { name: 'price', type: 'uint256' }, { name: 'size', type: 'uint256' }, { name: 'deadline', type: 'uint256' },
  { name: 'nonce', type: 'uint256' }, { name: 'salt', type: 'uint256' }, { name: 'postOnly', type: 'bool' }, { name: 'ioc', type: 'bool' },
] };
export const BOOK_ABI = [
  { type: 'function', name: 'settle', stateMutability: 'nonpayable', inputs: [{ name: 'fills', type: 'tuple[]', components: [{ name: 'maker', ...L3_ORDER_TUPLE }, { name: 'taker', ...L3_ORDER_TUPLE }, { name: 'price', type: 'uint256' }, { name: 'size', type: 'uint256' }] }, { name: 'makerSigs', type: 'bytes[]' }, { name: 'takerSigs', type: 'bytes[]' }], outputs: [] },
  { type: 'function', name: 'cancel', stateMutability: 'nonpayable', inputs: [{ name: 'o', ...L3_ORDER_TUPLE }], outputs: [] },
  { type: 'function', name: 'cancelMany', stateMutability: 'nonpayable', inputs: [{ name: 'os', type: 'tuple[]', components: L3_ORDER_TUPLE.components }], outputs: [] },
  { type: 'function', name: 'bumpNonce', stateMutability: 'nonpayable', inputs: [], outputs: [] },
  { type: 'function', name: 'setMinNonce', stateMutability: 'nonpayable', inputs: [{ type: 'uint256' }], outputs: [] },
  { type: 'function', name: 'hashOrder', stateMutability: 'view', inputs: [{ name: 'o', ...L3_ORDER_TUPLE }], outputs: [{ type: 'bytes32' }] },
  { type: 'function', name: 'remaining', stateMutability: 'view', inputs: [{ name: 'o', ...L3_ORDER_TUPLE }], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'filled', stateMutability: 'view', inputs: [{ type: 'bytes32' }], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'cancelled', stateMutability: 'view', inputs: [{ type: 'bytes32' }], outputs: [{ type: 'bool' }] },
  { type: 'function', name: 'minNonce', stateMutability: 'view', inputs: [{ type: 'address' }], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'desk', stateMutability: 'view', inputs: [], outputs: [{ type: 'address' }] },
  { type: 'function', name: 'domainSeparator', stateMutability: 'view', inputs: [], outputs: [{ type: 'bytes32' }] },
  { type: 'event', name: 'Filled', inputs: [{ name: 'makerHash', type: 'bytes32', indexed: true }, { name: 'takerHash', type: 'bytes32', indexed: true }, { name: 'maker', type: 'address', indexed: true }, { name: 'taker', type: 'address', indexed: false }, { name: 'price', type: 'uint256', indexed: false }, { name: 'size', type: 'uint256', indexed: false }, { name: 'takerBuys', type: 'bool', indexed: false }] },
  { type: 'event', name: 'Cancelled', inputs: [{ name: 'orderHash', type: 'bytes32', indexed: true }, { name: 'user', type: 'address', indexed: true }, { name: 'by', type: 'address', indexed: false }] },
  { type: 'event', name: 'NonceBumped', inputs: [{ name: 'user', type: 'address', indexed: true }, { name: 'minNonce', type: 'uint256', indexed: false }] },
  { type: 'event', name: 'Settled', inputs: [{ name: 'fills', type: 'uint256', indexed: false }, { name: 'moves', type: 'uint256', indexed: false }] },
];
/// a limit order message (price and size in wei as bigint; `token` = the wrapped outcome token the desk holds)
export function l3OrderFor({ user, marketId, outcome = 0, token, buy, price, size, ttlSec = 7 * 86400, postOnly = false, ioc = false }) {
  return {
    user, marketId: BigInt(marketId), outcome: BigInt(outcome), token, buy: !!buy, price: BigInt(price), size: BigInt(size),
    deadline: BigInt(Math.floor(Date.now() / 1000) + ttlSec), nonce: BigInt(Date.now()), salt: BigInt(Math.floor(Math.random() * 1e15)) * 1000003n + BigInt(Math.floor(Math.random() * 1e6)),
    postOnly: !!postOnly, ioc: !!ioc,
  };
}
export async function signL3OrderWithSession(sessionPk, domain, order) {
  return privateKeyToAccount(sessionPk).signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: order });
}
export async function signL3OrderWithAccount(account, domain, order) {
  return account.signTypedData({ domain, types: L3_ORDER_TYPES, primaryType: 'L3Order', message: order });
}
export const serializeL3Order = (o) => ({ user: o.user, marketId: String(o.marketId), outcome: String(o.outcome), token: o.token, buy: !!o.buy, price: String(o.price), size: String(o.size), deadline: String(o.deadline), nonce: String(o.nonce), salt: String(o.salt), postOnly: !!o.postOnly, ioc: !!o.ioc });
export const parseL3Order = (j) => ({ user: j.user, marketId: BigInt(j.marketId), outcome: BigInt(j.outcome || 0), token: j.token, buy: !!j.buy, price: BigInt(j.price), size: BigInt(j.size), deadline: BigInt(j.deadline), nonce: BigInt(j.nonce), salt: BigInt(j.salt || 0), postOnly: !!j.postOnly, ioc: !!j.ioc });
/// off-chain cancels on the L3 book are plain signed messages (by the user or the session key); on-chain cancel/bumpNonce stay available
export const l3CancelMessage = (hash, ts) => `RollMarkets L3 cancel ${hash} ${ts}`;
export const l3CancelAllMessage = (ts) => `RollMarkets L3 cancel-all ${ts}`;
export async function signL3CancelWithSession(sessionPk, hash, ts) { return privateKeyToAccount(sessionPk).signMessage({ message: l3CancelMessage(hash, ts) }); }
export async function signL3CancelAllWithSession(sessionPk, ts) { return privateKeyToAccount(sessionPk).signMessage({ message: l3CancelAllMessage(ts) }); }

// ---- miner finality (RollaBookL3.sol, docs/L3-MINERS.md §4) ----------------------------------------
// RollaBookL3 is RollaBook plus two functions. `attest(L3Final, sigs)` records a batch root once `threshold`
// distinct staked miners (RollaL3Miners) signed the EIP-712 `L3Final` below — same domain as the orders
// (`RollaBook`/`1`, verifyingContract = the RollaBookL3 address), signatures sorted by signer ascending, in chain
// order per shard. `settleFromRoot(shardId, index, RootFill[])` then settles fills with everything `settle`
// checks AND a Merkle proof per fill against the finalized fillsRoot (engine/l3/miner/merkle.js's tree),
// consuming the leaf. shardId = keccak256(utf8(shard)) (merkle.js shardId()).
export const L3_FINAL_TYPES = {
  L3Final: [
    { name: 'shardId', type: 'bytes32' }, { name: 'index', type: 'uint64' }, { name: 'batchRoot', type: 'bytes32' },
    { name: 'prevRoot', type: 'bytes32' }, { name: 'fillsRoot', type: 'bytes32' }, { name: 'fills', type: 'uint32' },
  ],
};
const L3_FINAL_TUPLE = { type: 'tuple', components: [
  { name: 'shardId', type: 'bytes32' }, { name: 'index', type: 'uint64' }, { name: 'batchRoot', type: 'bytes32' },
  { name: 'prevRoot', type: 'bytes32' }, { name: 'fillsRoot', type: 'bytes32' }, { name: 'fills', type: 'uint32' },
] };
const L3_FILL_TUPLE = { type: 'tuple', components: [{ name: 'maker', ...L3_ORDER_TUPLE }, { name: 'taker', ...L3_ORDER_TUPLE }, { name: 'price', type: 'uint256' }, { name: 'size', type: 'uint256' }] };
const L3_PROOF_TUPLE = { type: 'tuple', components: [{ name: 'seq', type: 'uint64' }, { name: 'index', type: 'uint32' }, { name: 'path', type: 'bytes32[]' }] };
export const BOOK_L3_ABI = [
  ...BOOK_ABI,
  { type: 'function', name: 'attest', stateMutability: 'nonpayable', inputs: [{ name: 'a', ...L3_FINAL_TUPLE }, { name: 'sigs', type: 'bytes[]' }], outputs: [] },
  { type: 'function', name: 'settleFromRoot', stateMutability: 'nonpayable', inputs: [{ name: 'shardId', type: 'bytes32' }, { name: 'index', type: 'uint64' }, { name: 'items', type: 'tuple[]', components: [{ name: 'fill', ...L3_FILL_TUPLE }, { name: 'proof', ...L3_PROOF_TUPLE }, { name: 'makerSig', type: 'bytes' }, { name: 'takerSig', type: 'bytes' }] }], outputs: [] },
  { type: 'function', name: 'finalDigest', stateMutability: 'view', inputs: [{ name: 'a', ...L3_FINAL_TUPLE }], outputs: [{ type: 'bytes32' }] },
  { type: 'function', name: 'finals', stateMutability: 'view', inputs: [{ type: 'bytes32' }, { type: 'uint64' }], outputs: [{ name: 'batchRoot', type: 'bytes32' }, { name: 'prevRoot', type: 'bytes32' }, { name: 'fillsRoot', type: 'bytes32' }, { name: 'fills', type: 'uint32' }, { name: 'attestedAt', type: 'uint64' }] },
  { type: 'function', name: 'anchored', stateMutability: 'view', inputs: [{ type: 'bytes32' }], outputs: [{ type: 'bool' }] },
  { type: 'function', name: 'nextIndex', stateMutability: 'view', inputs: [{ type: 'bytes32' }], outputs: [{ type: 'uint64' }] },
  { type: 'function', name: 'lastRoot', stateMutability: 'view', inputs: [{ type: 'bytes32' }], outputs: [{ type: 'bytes32' }] },
  { type: 'function', name: 'isSettled', stateMutability: 'view', inputs: [{ type: 'bytes32' }, { type: 'uint64' }, { type: 'uint32' }], outputs: [{ type: 'bool' }] },
  { type: 'function', name: 'fillLeaf', stateMutability: 'pure', inputs: [{ type: 'uint256' }, { type: 'bytes32' }, { type: 'bytes32' }, { type: 'address' }, { type: 'address' }, { type: 'uint256' }, { type: 'uint256' }, { type: 'bool' }], outputs: [{ type: 'bytes32' }] },
  { type: 'function', name: 'verifyFillProof', stateMutability: 'pure', inputs: [{ type: 'bytes32' }, { type: 'uint32' }, { type: 'uint32' }, { type: 'bytes32[]' }, { type: 'bytes32' }], outputs: [{ type: 'bool' }] },
  { type: 'function', name: 'miners', stateMutability: 'view', inputs: [], outputs: [{ type: 'address' }] },
  { type: 'function', name: 'FINAL_TYPEHASH', stateMutability: 'view', inputs: [], outputs: [{ type: 'bytes32' }] },
  { type: 'event', name: 'Attested', inputs: [{ name: 'shardId', type: 'bytes32', indexed: true }, { name: 'index', type: 'uint64', indexed: true }, { name: 'batchRoot', type: 'bytes32', indexed: false }, { name: 'fillsRoot', type: 'bytes32', indexed: false }, { name: 'fills', type: 'uint32', indexed: false }, { name: 'signers', type: 'uint256', indexed: false }] },
  { type: 'event', name: 'SettledFromRoot', inputs: [{ name: 'shardId', type: 'bytes32', indexed: true }, { name: 'index', type: 'uint64', indexed: true }, { name: 'fills', type: 'uint256', indexed: false }, { name: 'moves', type: 'uint256', indexed: false }] },
];
/// the staked miner set (RollaL3Miners.sol): what the engine reads before it attests
export const L3_MINERS_ABI = [
  { type: 'function', name: 'threshold', stateMutability: 'view', inputs: [], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'minStake', stateMutability: 'view', inputs: [], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'isMiner', stateMutability: 'view', inputs: [{ type: 'address' }], outputs: [{ type: 'bool' }] },
  { type: 'function', name: 'stakeOf', stateMutability: 'view', inputs: [{ type: 'address' }], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'allMiners', stateMutability: 'view', inputs: [], outputs: [{ type: 'address[]' }] },
  { type: 'function', name: 'minerCount', stateMutability: 'view', inputs: [], outputs: [{ type: 'uint256' }] },
  { type: 'function', name: 'register', stateMutability: 'payable', inputs: [], outputs: [] },
];
/// the L3Final message as the contract hashes it: index as a bigint (uint64), fills as a number (uint32)
export const l3FinalMessage = (a) => ({ shardId: a.shardId, index: BigInt(a.index), batchRoot: a.batchRoot, prevRoot: a.prevRoot, fillsRoot: a.fillsRoot, fills: Number(a.fills) });
/// a miner's finality signature over a batch: EIP-712 L3Final under BOOK_DOMAIN(chainId, bookL3). Equals
/// RollaBookL3.finalDigest(a) signed — pinned against the deployed contract in engine/l3/l3-root.test.mjs.
export async function signL3Final(account, domain, a) {
  return account.signTypedData({ domain, types: L3_FINAL_TYPES, primaryType: 'L3Final', message: l3FinalMessage(a) });
}
