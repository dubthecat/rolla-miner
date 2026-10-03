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
docker run --rm -e L3_LOG=kafka -e L3_KAFKA_BROKERS=broker:9092 -e L3_SHARD=1 -e L3_MINER_KEY=0x… -p 8080:8080 \
  ghcr.io/dubthecat/rolla-miner:latest
```

| env | meaning |
| --- | --- |
| `L3_LOG` | `kafka` (default in the image) or `file` (append-only JSONL under `data/`, single node / tests) |
| `L3_KAFKA_BROKERS` | comma-separated brokers (Redpanda works: see `docker-compose.yml`) |
| `L3_SHARD` | the market shard this miner follows (topics `orders.<shard>`, `votes.<shard>`) |
| `L3_MINER_KEY` | the miner's signing key (its votes are EIP-191 signatures; register the address with the validators) |
| `L3_NATIVE` | `1` = the C++ core in-process (default in the image), `0` = the JavaScript reference |
| `PORT` | `/healthz` and `/metrics` (default 8080) |

Local cluster: `MINER1_KEY=0x… MINER2_KEY=0x… MINER3_KEY=0x… docker compose up` (one Redpanda, three miners).

## RunPod

The image is public; a miner is a CPU pod. `scripts/runpod-cluster.mjs` (RunPod API key in `RUNPOD_API_KEY`) starts one
Redpanda pod and N miner pods, feeds a workload, collects every miner's `/metrics` (log offset, finalized epoch, votes,
dissents, orders/s, signature cost) and terminates everything. Budget-capped.

## Develop

```
npm install
make -C native/book          # the core, tests, the differential check: make -C native/book test
npm test
```

Commits that change the book must keep `native/book/diff.mjs` green: the JavaScript matcher is the specification.
