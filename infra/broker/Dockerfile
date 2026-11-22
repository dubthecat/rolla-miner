# The L3 log's broker: one Redpanda node, one topic per shard (orders.<shard> and votes.<shard>).
#
# PINNED, deliberately. This was `redpandadata/redpanda:latest`, and the live cluster is v26.2.3 — so the next
# recreate of this machine would have pulled v26.3.x, where `protected_mode` defaults to true and refuses every
# non-loopback Kafka client. The whole miner fleet would have dropped off the log at the next rebuild, with
# "Closed connection" as the only symptom. Pin the version, and seed the cluster properties (bootstrap.yaml) so a
# fresh cluster is correct whatever the version does by default. Moving the pin forward is a deliberate act.
FROM redpandadata/redpanda:v26.2.3
USER root
# read once, when the cluster is first formed; an existing cluster keeps what it has
COPY bootstrap.yaml /etc/redpanda/.bootstrap.yaml
COPY start.sh /usr/local/bin/rolla-broker-start
RUN chmod 0644 /etc/redpanda/.bootstrap.yaml && chmod 0755 /usr/local/bin/rolla-broker-start && \
    chown redpanda:redpanda /etc/redpanda/.bootstrap.yaml
USER redpanda
ENTRYPOINT ["/bin/sh", "/usr/local/bin/rolla-broker-start"]
