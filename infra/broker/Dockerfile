# The public Redpanda image: its entrypoint is rpk, and `rpk redpanda start …` is the supported way to start a node with
# addresses on the command line. Advertises the Fly app's public TCP port (a dedicated IPv4 on 9092).
FROM redpandadata/redpanda:latest
CMD ["redpanda", "start", "--overprovisioned", "--smp", "3", "--memory", "6G", "--reserve-memory", "0M", "--node-id", "0", "--check=false", "--kafka-addr", "PLAINTEXT://0.0.0.0:9092", "--advertise-kafka-addr", "PLAINTEXT://rolla-l3-broker.fly.dev:9092", "--set", "redpanda.kafka_batch_max_bytes=8388608"]
# NOTE: `--set` only seeds the FIRST boot. Redpanda keeps cluster properties centrally on the volume, so the limit was applied with
#   fly ssh console -a rolla-l3-broker -C "rpk cluster config set kafka_batch_max_bytes 8388608"   (2026-10-03; before that, 1.2 MB appends were refused at the 1 MiB default)
