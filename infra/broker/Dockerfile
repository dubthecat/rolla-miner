# ghcr-free: the public Redpanda image, advertising the Fly app's public TCP port (a dedicated IPv4 on 9092).
FROM redpandadata/redpanda:latest
ENV ADVERTISE=rolla-l3-broker.fly.dev
CMD ["sh", "-c", "exec redpanda start --overprovisioned --smp 1 --memory 700M --reserve-memory 0M --node-id 0 --check=false --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://${ADVERTISE}:9092"]
