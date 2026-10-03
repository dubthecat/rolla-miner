# The public Redpanda image: its entrypoint is rpk, and `rpk redpanda start …` is the supported way to start a node with
# addresses on the command line. Advertises the Fly app's public TCP port (a dedicated IPv4 on 9092).
FROM redpandadata/redpanda:latest
CMD ["redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "700M", "--reserve-memory", "0M", "--node-id", "0", "--check=false", "--kafka-addr", "PLAINTEXT://0.0.0.0:9092", "--advertise-kafka-addr", "PLAINTEXT://rolla-l3-broker.fly.dev:9092"]
