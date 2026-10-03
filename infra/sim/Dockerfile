# ghcr.io/dubthecat/rolla-miner-sim — the miner image plus a Redpanda broker, for one self-contained "venue cell":
# scripts/sim-cell.mjs starts the broker on localhost, S sequencer threads and S×R miner processes in one machine.
FROM ghcr.io/dubthecat/rolla-miner:latest
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates gnupg util-linux && \
    curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | bash && \
    apt-get install -y --no-install-recommends redpanda && rm -rf /var/lib/apt/lists/* && rpk version
ENV SIM_LOCAL_BROKER=1
CMD ["node", "scripts/sim-cell.mjs"]
