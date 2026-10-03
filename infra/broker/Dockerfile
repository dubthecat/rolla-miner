# The public Redpanda image (its entrypoint is rpk, so the entrypoint is cleared and redpanda is run directly),
# advertising the Fly app's public TCP port: a dedicated IPv4 on 9092 (rolla-l3-broker.fly.dev).
FROM redpandadata/redpanda:latest
ENTRYPOINT []
CMD ["/usr/bin/redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "700M", "--reserve-memory", "0M", "--node-id", "0", "--check=false", "--kafka-addr", "PLAINTEXT://0.0.0.0:9092", "--advertise-kafka-addr", "PLAINTEXT://rolla-l3-broker.fly.dev:9092"]
