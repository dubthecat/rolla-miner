# rolla-miner

One **L3 miner** for [RollMarkets](https://rollmarkets.com): it replays the sequenced order log of a market shard into its
own deterministic price-time book, hashes the result, and votes. When enough miners publish matching votes a batch is
final; the settlement validators attest finalized roots and the chain settles the fills against the users' own
signatures. No miner can invent, reorder or block a fill without the others seeing a different hash.

- **Book core:** `native/book` — a C++20 limit order book (flat price levels by tick, intrusive FIFO, O(1) cancel,
  running transcript hash and book hash). 1.6–2.8 M ops/s per core, p99 under 2 µs. `src/matcher.js` is the JavaScript
  reference with the same semantics; `native/book/diff.mjs` proves they agree fill for fill.
- **Miner:** `src/miner/` — consumes a shard's batches from the log (Kafka / Redpanda, or a file log for tests), applies
  them (native core in-process, `L3_NATIVE=1`), recomputes the batch's Merkle `fillsRoot` and `bookHash`, publishes a
  signed vote or dissent, keeps its own journal, serves `/healthz` and `/metrics`.
- **Design:** `docs/L3-MINERS.md` in the [proto](https://github.com/dubthecat/proto) monorepo (sequencer, miners,
  validators, finality, challenge proofs, failure modes).

The book semantics live in the monorepo's engine; `scripts/sync-from-proto.sh` copies them here. This repo exists so the
miner builds as a public image and runs anywhere with one command.

## Run

```
docker run --rm -e L3_LOG=kafka -e L3_KAFKA_BROKERS=broker:9092 -e L3_SHARD=1 -e L3_MINER_KEY=0x… \
  -e PREDICT_BOOK=0x7197A5160562516F6f8C4503dF03CD836a524D66 -e CHAIN_ID=46630 -p 8080:8080 \
  ghcr.io/dubthecat/rolla-miner:latest
```

| env | meaning |
| --- | --- |
| `L3_LOG` | `kafka` (default in the image) or `file` (append-only JSONL, single node / tests) |
| `PREDICT_BOOK` + `CHAIN_ID` | the RollaBook address and chain id: half of the EIP-712 domain the orders were signed under — a wrong value dissents on every batch (checked at boot) |
| `L3_SEQUENCERS` | addresses allowed to seal batches (empty = any signer, and the miner says so at boot) |
| `L3_THRESHOLD` | how many agreeing miners this miner considers final (default 2) |
| `L3_VERIFY_WORKERS` | signature-verification threads (default min(4, cpus−1)); ~3.7k orders/s per worker with the native bindings |
| `DATA_DIR` | the miner's journal and log copy (`/data` in the image) |
| `L3_KAFKA_BROKERS` | comma-separated brokers (Redpanda works: see `docker-compose.yml`) |
| `L3_SHARD` | ONE market shard this miner follows (topics `orders.<shard>`, `votes.<shard>`) — or, without it, fleet mode: |
| `L3_SHARDS` | a comma-separated list of shards, every one mined by this process (one log connection, one verifier, a miner per shard, one `/healthz`) |
| `L3_ENGINE_URL` | follow the engine: `GET <url>/v1/l3/markets` every `L3_MARKETS_POLL_MS` (60000) lists its books; a miner starts for every shard that appears and stops for one that disappears (`L3_SHARDS` entries never stop). `L3_SHARD_BY=outcome` mirrors an engine that shards by book |
| `L3_LOG_BOOT_RETRY_MS` | fleet mode: the wait between attempts to reach the broker at boot (5000); `/healthz` is 503 meanwhile, not a crash loop |
| `L3_MINER_KEY` | the miner's signing key (its votes are EIP-191 signatures; register the address with the validators) |
| `L3_NATIVE` | `1` = replay in the native book process (tick-grid markets), default the JavaScript reference |
| `PORT` | `/healthz` and `/metrics` (default 8080) |

Local cluster: `MINER1_KEY=0x… MINER2_KEY=0x… MINER3_KEY=0x… docker compose up` (one Redpanda, three miners).

## Fly fleet (production)

Three always-on miners — `rollmarkets-miner-1/2/3` in `ams`, one app and one shared-cpu-2x / 1 GB machine each, the
three keys registered on `RollaL3Miners` `0x4a95…1fe0` (threshold 2) — run this image in **fleet mode** from
`infra/miner/fly.toml`: they follow `https://rollmarkets.com/v1/l3/markets` every minute and mine every listed shard off
the Fly broker, sign `L3Final` for `RollaBookL3`, and answer `/healthz` + `/metrics` at `https://rollmarkets-miner-N.fly.dev/`.
`infra/miner/deploy.sh N` creates and deploys one (idempotent; the key is read from `~/.rollacoasta-keys/l3-miner-N.key`
and never printed), `infra/miner/status.sh` prints the three health lines, `infra/miner/RUNBOOK.md` has the rest:
rolling the image, rotating a key, re-staking, the staged test, the engine flip. About $6.55 a month per miner.

## RunPod

The image is public; a miner is a CPU pod. The log broker runs on Fly (`infra/broker`: Redpanda behind a dedicated IPv4 on 9092, `rolla-l3-broker.fly.dev:9092`) because RunPod's public TCP mappings on CPU pods did not answer from outside in testing while its HTTP proxy did; miners only need outbound connections. `scripts/runpod-cluster.mjs` (RunPod API key in `RUNPOD_API_KEY`) starts one
Redpanda pod and N miner pods, feeds a workload, collects every miner's `/metrics` (log offset, finalized epoch, votes,
dissents, orders/s, signature cost) and terminates everything. Budget-capped.

## Develop

```
npm install
make -C native/book          # the core, tests, the differential check: make -C native/book test
npm test
```

Commits that change the book must keep `native/book/diff.mjs` green: the JavaScript matcher is the specification.

### Broker message limit

Redpanda keeps cluster properties centrally on its volume, so the `--set redpanda.kafka_batch_max_bytes=8388608` in `infra/broker/Dockerfile` only seeds the first boot. The running broker was set with `fly ssh console -a rolla-l3-broker -C "rpk cluster config set kafka_batch_max_bytes 8388608"`; verify with `rpk cluster config get kafka_batch_max_bytes`. An op is ~600 B, so `--batch 2000` is ~1.2 MB per message. The append round trip from a machine outside Fly is ~260 ms whatever the size, and the sequencer seals one batch per round trip: `--batch` is the sequencing-rate knob in this test.
