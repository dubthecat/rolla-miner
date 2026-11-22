#!/bin/bash
# infra/miner/join.sh NAME — put a miner of your own on the live venue, from nothing, in one command.
#
#   export PATH=$PATH:$HOME/.fly/bin
#   export FLY_API_TOKEN=$(fly auth token)
#   bash infra/miner/join.sh alice
#
# That creates the Fly app `rolla-miner-alice`, a 1 GB volume for its journal, a key of its own if it has none,
# and deploys the public image. It is idempotent: run it again to upgrade, and nothing is recreated.
#
# What a miner does, so you know what you are running: it follows the venue's shard list, reads every sealed batch
# off the log, re-verifies every order signature in it, applies them to its own copy of the book and signs a vote
# for the root it computed. It never takes the sequencer's word for anything. Two of three agreeing is finality.
# It holds no funds, needs no chain access and exposes nothing but /healthz and /metrics.
#
# What to send the operator afterwards: the ADDRESS this prints. It goes in the engine's L3_MINERS_ALLOW (so your
# votes count towards on-chain attestation) and is where your µROLLA is credited. Your key never leaves the box.
#
#   bash infra/miner/join.sh alice --shards 19085,19086   # a fixed shard list instead of following the engine
#   L3_KAFKA_BROKERS=… L3_ENGINE_URL=… bash infra/miner/join.sh bob   # a different venue (a testnet, a bench)
#   SIZE=performance-4x WORKERS=3 bash infra/miner/join.sh carol      # a bigger machine for hot shards
set -euo pipefail
NAME=${1:?usage: join.sh NAME [--shards a,b,c]}
shift || true
SHARDS=""
while [ $# -gt 0 ]; do case "$1" in --shards) SHARDS=${2:?}; shift 2;; *) echo "unknown argument $1" >&2; exit 2;; esac; done

# was BOOK_L3 given, or are we about to default it? (checked before the default is applied, below)
BOOK_GIVEN=${BOOK_L3+yes}
BROKERS_GIVEN=${L3_KAFKA_BROKERS+yes}
ENGINE_GIVEN=${L3_ENGINE_URL+yes}

APP=${APP:-rolla-miner-$NAME}
REGION=${REGION:-ams}
ORG=${FLY_ORG:-personal}
SIZE=${SIZE:-shared-cpu-2x}
MEMORY=${MEMORY:-2048}
WORKERS=${WORKERS:-2}
VOLSIZE=${VOLSIZE:-1}
IMAGE=${IMAGE:-ghcr.io/dubthecat/rolla-miner:latest}
KEY=${L3_MINER_KEY_FILE:-$HOME/.rollacoasta-keys/l3-miner-$NAME.key}
# the live venue's defaults; override any of them for another deployment
BROKERS=${L3_KAFKA_BROKERS:-rolla-l3-broker.fly.dev:9092}
ENGINE=${L3_ENGINE_URL:-https://rollmarkets.com}
SEQUENCERS=${L3_SEQUENCERS:-0x4f2689f90a854173d5AC9Ac44c318D79011D3267}
BOOK_L3=${BOOK_L3:-0xfc39ab228ec5a77e681f2fa3c9491ca07456c9bc}
THRESHOLD=${L3_THRESHOLD:-2}
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)

: "${FLY_API_TOKEN:?export FLY_API_TOKEN=\$(fly auth token) first}"
command -v fly >/dev/null || { echo "fly is not on PATH — install flyctl, then export PATH=\$PATH:\$HOME/.fly/bin" >&2; exit 1; }
[[ "$BOOK_L3" =~ ^0x[0-9a-fA-F]{40}$ ]] || { echo "BOOK_L3 is not an address" >&2; exit 1; }

# The trap this script exists to close. PREDICT_BOOK is half of the EIP-712 domain every order is signed under, so
# a miner pointed at one venue's broker while checking another venue's book recomputes a different hash for every
# order and dissents on EVERY batch, from the first one, forever. There is no other symptom: the machine is
# healthy, the log is flowing, the roots "differ". It cost a benchmark run to find.
# So: the defaults below are the LIVE venue's, and asking for a different broker or engine without also naming the
# book is refused rather than guessed.
if [ -n "${BROKERS_GIVEN:-}" ] || [ -n "${ENGINE_GIVEN:-}" ]; then
  if [ -z "${BOOK_GIVEN:-}" ]; then
    cat >&2 <<'MSG'
error: you pointed this miner at a different venue (L3_KAFKA_BROKERS / L3_ENGINE_URL) but did not say which book
       its orders are signed under, and the default is the LIVE venue's.

       PREDICT_BOOK is half of the EIP-712 domain. Get it wrong and this miner dissents on every batch forever
       while looking perfectly healthy. Set it explicitly:

         BOOK_L3=0x… L3_KAFKA_BROKERS=… bash infra/miner/join.sh NAME

       The venue operator publishes that address; it is the RollaBookL3 the orders are signed under.
MSG
    exit 2
  fi
fi

# 1. an identity. Made here if absent, never printed, never overwritten.
ADDR=$(node "$REPO/scripts/new-key.mjs" "$KEY")
echo "miner $NAME is $ADDR"

# 2. the app
if fly status -a "$APP" >/dev/null 2>&1; then echo "app $APP exists"; else fly apps create "$APP" --org "$ORG"; fi

# 3. the volume: the journal is this miner's own copy of the log, which is what makes it a data-availability node
if fly volumes list -a "$APP" 2>/dev/null | grep -q minerdata; then echo "volume minerdata exists"
else fly volumes create minerdata -a "$APP" -r "$REGION" -s "$VOLSIZE" -y; fi

# 4. the key, staged so the deploy below applies it. Read from the file straight into the command; nothing printed.
fly secrets set -a "$APP" --stage L3_MINER_KEY="$(tr -d '[:space:]' < "$KEY")" >/dev/null 2>&1 && echo "key staged as a secret"

# 5. deploy. --ha=false is one machine: a miner is one machine with one journal.
ENVS=(--env "L3_KAFKA_BROKERS=$BROKERS" --env "L3_SEQUENCERS=$SEQUENCERS" --env "L3_THRESHOLD=$THRESHOLD"
      --env "PREDICT_BOOK=$BOOK_L3" --env "PREDICT_BOOK_L3=$BOOK_L3" --env "L3_VERIFY_WORKERS=$WORKERS")
# a fixed shard list OR the engine's — a miner needs one of the two, and a bench venue has no engine
if [ -n "$SHARDS" ]; then ENVS+=(--env "L3_SHARDS=$SHARDS" --env "L3_ENGINE_URL="); else ENVS+=(--env "L3_ENGINE_URL=$ENGINE"); fi

fly deploy --config "$HERE/fly.toml" --app "$APP" --image "$IMAGE" --ha=false \
  --vm-size "$SIZE" --vm-memory "$MEMORY" "${ENVS[@]}"

echo
echo "joined. health: fly ssh console -a $APP -C \"curl -s http://127.0.0.1:8080/healthz\""
echo "send the operator this address so your votes count and your µROLLA is credited: $ADDR"
