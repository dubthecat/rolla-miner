# Miner fleet runbook

Everything below starts with

```
export PATH=$PATH:$HOME/.fly/bin; export FLY_API_TOKEN=$(cat ~/.rollacoasta-keys/fly-rollmarkets.token)
cd ~/new/rolla-miner
```

Apps `rollmarkets-miner-1/2/3` (ams, shared-cpu-2x, 1 GB, volume `minerdata` 1 GB at `/data`). The token is an org token:
never print it, never paste it in a chat or a commit. The miner keys live only in `~/.rollacoasta-keys/l3-miner-N.key`
and in each app's `L3_MINER_KEY` secret.

## Is it alive?

```
bash infra/miner/status.sh                 # one line per miner from /healthz: ok, shards, index/finalIndex per shard, polls
bash infra/miner/status.sh --fly 2         # plus the machine table
fly logs -a rollmarkets-miner-2            # JSON lines: "log connected", "[l3fleet] mining <shard>", a heartbeat a minute
curl -s https://rollmarkets-miner-1.fly.dev/metrics | grep -v '^#'
```

`/healthz` is 200 while every shard is sound. It is 503 — and Fly marks the machine unhealthy — when a miner **stalled**
(a batch from an unknown sequencer, a gap in the log, a batch that threw), **dissented** (its root differs from the
sequencer's: that is a fork, settlement of that shard halts until it is judged), or has **votes the log refuses** (the
broker is down: `voteBacklog`). An idle shard is not a fault: `index -1`, `lagSeconds -1`, nothing sequenced yet. A failing
engine poll (`pollErrors`) is reported and logged but is not a fault either — the last shard list stands.

Metrics (Prometheus text, one series per shard, `rolla_l3_*`): `batches_total`, `votes_total`, `dissents_total`,
`bad_signatures_total`, `index`, `final_index`, `offset`, `lag_seconds`, `verify_ms_per_order`, `votes_pending`,
`stalled`, `halted`, `rewards_micro`; fleet-wide `fleet_shards`, `fleet_listed_shards`, `fleet_polls_total`,
`fleet_poll_errors_total`, `fleet_last_poll_ok_seconds`, `fleet_start_failures_total`, `fleet_ready`, `up`.

A stalled or dissenting shard does not clear itself — the miner refuses to go past the batch it disagrees with, by design.
Read `fly logs` for the `[l3miner] DISSENT …` / `stalled` line, decide who is wrong (docs/L3-MINERS.md §5, §7), and only
then restart: `fly machine restart <id> -a rollmarkets-miner-N`. A restart replays the shard from the miner's own journal
on `/data` and continues from the log; if the journal itself is the problem, `fly ssh console -a … -C "rm /data/l3-miner/<shard>.batches.jsonl"`
makes the miner re-read that shard from the log's beginning.

## Roll the image

Every push to `main` builds `ghcr.io/dubthecat/rolla-miner:latest` (and `:<sha>`). The apps pull on deploy, not by
themselves:

```
gh run watch                                    # the build is green
for N in 1 2 3; do bash infra/miner/deploy.sh $N; done     # rolling: one machine per app, ~1 min each
bash infra/miner/status.sh
```

Roll one at a time and look at `status.sh` in between: two miners down at once is no quorum (threshold 2 of 3), and the
engine's settlement of every shard waits until two are back. `IMAGE=ghcr.io/dubthecat/rolla-miner:<sha> bash infra/miner/deploy.sh N`
pins a known-good image to roll back.

## Change the book (a RollaBookL3 redeploy)

`PREDICT_BOOK` and `PREDICT_BOOK_L3` are half of the EIP-712 domain: a miner on the wrong address dissents on every
batch. After a redeploy: `BOOK_L3=0x<new> bash infra/miner/deploy.sh N` for each N, in step with the engine's
`PREDICT_BOOK_L3` secret. The old shards' journals stay on `/data` and are harmless (the engine names new markets).

## Rotate a key

A miner's key is also its registration and its stake: rotating one means a new miner on chain.

1. Make the new key: `node -e "const {generatePrivateKey,privateKeyToAccount}=require('viem/accounts');const k=generatePrivateKey();require('fs').writeFileSync(process.env.HOME+'/.rollacoasta-keys/l3-miner-N.new.key',k,{mode:0o600});console.log(privateKeyToAccount(k).address)"`
   (prints the address only). Back the file up where the other keys are backed up.
2. Fund it (0.002 ETH of testnet gas is plenty) and register it with `minStake` (0.001 ETH) on `RollaL3Miners`
   `0x4a95d4f170c059e30507fdd3d74de9fcb0fa1fe0`: `register()` payable from the new key (the proto monorepo's
   `rollmarkets/scripts/l3/deploy-root.mjs` does exactly this for the three keys; `cast send 0x4a95… "register()" --value 0.001ether --private-key …` is the one-liner).
   Threshold stays 2; with four registered miners the engine's `L3_MINERS_ALLOW` must list the new address, or its votes
   are ignored by the engine's quorum (the chain accepts any registered miner's `L3Final`).
3. Swap the app's secret — the machine restarts with the new identity, its journal on `/data` is reused (votes are keyed by
   miner, so the old address's journal lines are simply history):
   `fly secrets set -a rollmarkets-miner-N L3_MINER_KEY="$(cat ~/.rollacoasta-keys/l3-miner-N.new.key)" >/dev/null`
4. Retire the old one: `exit()` from the old key (it leaves the set now, stays slashable through the 86,400 s cooldown),
   then `withdraw()` after the cooldown. Remove it from the engine's `L3_MINERS_ALLOW`. Move the new file over the old
   name once the old key is withdrawn.

## Re-stake / top up

`topUp()` payable from the miner's key adds to its stake; a slash below `minStake` removes the miner from the set and starts
its cooldown — `withdraw()` after it, then `register()` again with a fresh stake (the registry refuses `register()`
while a residual stake is held: fix pass 2026-10-04 #6). `allMiners()` / `stakeOf(address)` / `isMiner(address)` on the
registry say where a miner stands.

## Staged test against the fleet (the engine is not touched)

`node scripts/predict/l3-e2e.mjs --fleet` in the proto monorepo (`rollmarkets/`) runs the engine's L3 runtime **in that
process**, with `L3_MINERS=1 L3_SETTLE=root L3_LOG=kafka` on the Fly broker and no local miners, creates a fresh market
(a new shard), and needs the fleet to mine it. The live engine's `/v1/l3/markets` cannot list a market the live engine has
no book for, so the script pins the shard on the three apps as a temporary secret and restores afterwards:

```
FLEET_SET_SHARDS=1 node scripts/predict/l3-e2e.mjs --fleet
#   → fly secrets set -a rollmarkets-miner-N L3_SHARDS=<market id> L3_SEQUENCERS=<engine operator>,<deployer>   (×3, restarts)
#   → the fill, the fleet's votes (2 of 3 is finality), attest + settleFromRoot on RollaBookL3, the tx hashes
#   → fly secrets unset -a rollmarkets-miner-N L3_SHARDS L3_SEQUENCERS   (×3, on exit — also on a failure)
```

`L3_SEQUENCERS` is widened because the local runtime seals batches with the deployer key, not the engine operator's.
Without `FLEET_SET_SHARDS=1` the script prints the two `fly secrets` lines and waits for the shard to appear on the
miners' `/healthz` before it trades. Either way check `bash infra/miner/status.sh` afterwards: three healthy miners, no
`L3_SHARDS` left (`fly secrets list -a rollmarkets-miner-N`).

## The engine flip (a later, coordinated step)

The fleet is ready before the engine is: today the live engine runs with `L3_MINERS` off and settles with `RollaBook.settle`.
Flipping it means the engine sequences every L3 order into the broker, waits for 2 of 3 miner votes per batch, and — with
`L3_SETTLE=root` — attests roots and settles from them on `RollaBookL3` + desk v3. On the engine app (`rollmarkets`):

```
fly secrets set -a rollmarkets --stage \
  L3_MINERS=1 L3_LOG=kafka L3_KAFKA_BROKERS=rolla-l3-broker.fly.dev:9092 L3_THRESHOLD=2 \
  L3_MINERS_ALLOW=0x33d02ce7DCb2e59DEEc508bb72646b3407514c7F,0x453A4Cda01434C7544699590804e2245949a9234,0x5adfaf2dDf50A4c329Fcbb5cEcd9E74FC9735C3b \
  L3_SETTLE=root PREDICT_BOOK_L3=0xa1d51c4e00926cb7b2e640bff13039d8849c4450 PREDICT_DESK_V3=0xda107fff2566688af84b70e6564fc85d51b44eb9
cd ~/new/proto/rollmarkets && fly deploy   # the engine's own deploy (see rollmarkets/docs/L3-MINERS.md §4 and §10)
```

Before: the fleet is healthy (`status.sh`), the broker is up (`fly status -a rolla-l3-broker`), the UI's `PREDICT.book` /
`PREDICT.deskV2` constants follow the flag (docs/L3-MINERS.md §4: orders are signed under RollaBookL3's domain under a
root — the miners' `PREDICT_BOOK` is already that address), and the fleet's `L3_SEQUENCERS` is the engine operator
(`0x4f2689f90a854173d5AC9Ac44c318D79011D3267`, the `OPERATOR_KEY` the engine seals batches with). Without `L3_SETTLE=root`
(just `L3_MINERS=1`) the engine still settles with `RollaBook.settle` on the live pair but only after the miners' quorum —
then the orders stay under the RollaBook domain and the fleet's `PREDICT_BOOK` must be `0x7197A5160562516F6f8C4503dF03CD836a524D66`
(`BOOK_L3=0x7197… bash infra/miner/deploy.sh N`, and `PREDICT_BOOK_L3` is then only the finality signature's address).
After: `/v1/l3/status` on the engine shows `miners` with the three addresses and `settle: 'root'`; the first order's
batch appears on the miners' `/healthz` as `index 0`, then `finalIndex 0`; the feed emits `l3:final`, `l3:attested`,
`l3:settled`. Rollback: `fly secrets unset -a rollmarkets L3_MINERS L3_SETTLE …` and redeploy — fills already staged
under a batch are settled by the engine's own `resumeFrom` on the next boot with miners, so flip back only when the
staged count on `/v1/l3/status` is 0.
