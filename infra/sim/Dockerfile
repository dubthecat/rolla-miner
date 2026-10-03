# ghcr.io/dubthecat/rolla-miner-sim — the miner image plus a Redpanda broker, for one self-contained "venue cell":
# scripts/sim-cell.mjs starts the broker on localhost, S sequencer threads and S×R miner processes in one machine.
# Redpanda's binaries come straight from its official image (its apt repo setup failed in CI); they carry their
# own libraries under /opt/redpanda/lib, so they run on this Debian base.
FROM redpandadata/redpanda:latest AS redpanda
FROM ghcr.io/dubthecat/rolla-miner:latest
USER root
COPY --from=redpanda /opt/redpanda /opt/redpanda
RUN useradd -r -m -d /var/lib/redpanda -s /usr/sbin/nologin redpanda 2>/dev/null || true; \
    ln -sf /opt/redpanda/bin/rpk /usr/local/bin/rpk && ln -sf /opt/redpanda/bin/redpanda /usr/local/bin/redpanda && \
    mkdir -p /etc/redpanda /var/lib/redpanda/data && chown -R redpanda:redpanda /etc/redpanda /var/lib/redpanda && rpk version
ENV PATH=/opt/redpanda/bin:$PATH SIM_LOCAL_BROKER=1
CMD ["node", "scripts/sim-cell.mjs"]
