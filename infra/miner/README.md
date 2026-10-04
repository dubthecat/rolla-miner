# The Fly miner fleet

Three independent L3 miners, each its own Fly app and machine in `ams`, each holding one of the three keys registered on
`RollaL3Miners` (`0x4a95d4f170c059e30507fdd3d74de9fcb0fa1fe0`, threshold 2). They run the public image this repo builds,
consume the engine's shard logs from the Fly broker (`rolla-l3-broker.fly.dev:9092`), replay every batch into their own
book, and publish signed votes — with the `L3Final` signature the chain's `RollaBookL3.attest` verifies.

| app | key | address |
| --- | --- | --- |
| `rollmarkets-miner-1` | `~/.rollacoasta-keys/l3-miner-1.key` | `0x33d02ce7DCb2e59DEEc508bb72646b3407514c7F` |
| `rollmarkets-miner-2` | `~/.rollacoasta-keys/l3-miner-2.key` | `0x453A4Cda01434C7544699590804e2245949a9234` |
| `rollmarkets-miner-3` | `~/.rollacoasta-keys/l3-miner-3.key` | `0x5adfaf2dDf50A4c329Fcbb5cEcd9E74FC9735C3b` |

- `fly.toml` — the template (shared-cpu-2x, 1 GB, a 1 GB volume at `/data`, `/healthz` check, public HTTPS for health
  and metrics only). The process runs `src/miner/run.mjs` in **fleet mode**: it follows `https://rollmarkets.com/v1/l3/markets`
  every minute and mines one miner per listed shard (a shard = a market id, as the engine's sequencer names them).
- `deploy.sh N` — idempotent: app, volume, the key secret (read from the file, never printed), deploy. `BOOK_L3=0x…`
  points `PREDICT_BOOK` / `PREDICT_BOOK_L3` at a redeployed `RollaBookL3`; `IMAGE=…:<sha>` pins an image.
- `status.sh` — the three `/healthz` on one line each (`--fly` adds the machine tables).
- `RUNBOOK.md` — rotate a key, re-stake, roll the image, read the metrics, the staged test, the engine flip.

```
export PATH=$PATH:$HOME/.fly/bin; export FLY_API_TOKEN=$(cat ~/.rollacoasta-keys/fly-rollmarkets.token)
for N in 1 2 3; do bash infra/miner/deploy.sh $N; done
bash infra/miner/status.sh
```

Cost: shared-cpu-2x with 1 GB is about $6.40/month per machine on Fly's published list plus $0.15 for the 1 GB volume —
≈ $6.55 per miner, ≈ $19.65 for the fleet (egress to the broker is negligible at today's volumes). The broker is a separate
app (`infra/broker`).

An idle fleet is healthy: a miner on a shard the engine has not sequenced yet sits on an empty topic (the topic is created
when the first side subscribes) and reports `index -1`. Finality needs the engine to run with `L3_MINERS=1` on the same
broker — see `RUNBOOK.md` "The engine flip".
