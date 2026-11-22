// scripts/new-key.mjs — a miner's identity, made once and kept.
//
//   node scripts/new-key.mjs                      # print a fresh address and write the key beside it
//   node scripts/new-key.mjs ~/.keys/my-miner.key  # a path of your choosing
//   node scripts/new-key.mjs --address-only <file>  # just tell me whose key this is
//
// The key is written with mode 0600 and NEVER printed — only the address, which is what you give the venue
// operator (the sequencer allowlist, and where your µROLLA is credited). A miner's key signs its votes; it holds
// no funds and it is not a wallet, but anyone with it can vote as you, so it stays on the machine and in your
// own backup.
import fs from 'node:fs';
import path from 'node:path';
import { generatePrivateKey, privateKeyToAccount } from 'viem/accounts';

const args = process.argv.slice(2).filter((a) => a !== '--address-only');
const addressOnly = process.argv.includes('--address-only');
const file = args[0] || path.join(process.env.HOME || '.', '.rollacoasta-keys', `l3-miner-${Date.now().toString(36)}.key`);

if (addressOnly) {
  const key = fs.readFileSync(file, 'utf8').trim();
  console.log(privateKeyToAccount(key).address);
  process.exit(0);
}
if (fs.existsSync(file)) {
  // never overwrite an identity: a miner that changes address loses its history and its credit
  console.log(privateKeyToAccount(fs.readFileSync(file, 'utf8').trim()).address);
  console.error(`(existing key at ${file} — left alone)`);
  process.exit(0);
}
fs.mkdirSync(path.dirname(file), { recursive: true });
const key = generatePrivateKey();
fs.writeFileSync(file, key + '\n', { mode: 0o600 });
console.log(privateKeyToAccount(key).address);
console.error(`key written to ${file} (mode 0600) — back it up; it is not recoverable`);
