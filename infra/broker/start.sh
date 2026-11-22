#!/bin/sh
# The L3 log's broker. Started this way rather than from a bare CMD for one reason: the address a broker
# ADVERTISES is the address every miner will dial, so it cannot be baked into the image if a second broker is ever
# to exist — and the scale benchmark says one has to (docs/L3-MINERS.md §7: a single broker flattens at about two
# sequencing cells, and the log is already one topic per shard, so shards divide over brokers cleanly).
#
#   BROKER_ADVERTISE   host:port the miners dial.  Default: the production app's public address, unchanged.
#   BROKER_SMP         cores seastar takes.        Default 3, which is what performance-4x leaves room for.
#   BROKER_MEMORY      memory seastar takes.       Default 6G of the machine's 8.
set -e
ADV="${BROKER_ADVERTISE:-rolla-l3-broker.fly.dev:9092}"
SMP="${BROKER_SMP:-3}"
MEM="${BROKER_MEMORY:-6G}"
# 0.0.0.0 is IPv4 and Fly's private network is IPv6-only, so a broker meant to be reached over 6PN must bind [::].
# The public listener (a dedicated IPv4 on the app) works either way, and [::] accepts both here.
BIND="${BROKER_BIND:-[::]}"
echo "[broker] advertising ${ADV} · binding ${BIND}:9092 · smp ${SMP} · memory ${MEM}"
exec rpk redpanda start \
  --overprovisioned --smp "${SMP}" --memory "${MEM}" --reserve-memory 0M --node-id 0 --check=false \
  --kafka-addr "PLAINTEXT://${BIND}:9092" --advertise-kafka-addr "PLAINTEXT://${ADV}" \
  --rpc-addr 127.0.0.1:33145
