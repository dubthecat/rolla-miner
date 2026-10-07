#!/bin/bash
# infra/miner/deploy.sh N — create (if missing) and deploy rollmarkets-miner-N in ams from the public image. Idempotent:
# the app, its volume and its key secret are created only when absent; the deploy itself is a rolling update.
#
#   export PATH=$PATH:$HOME/.fly/bin; export FLY_API_TOKEN=$(cat ~/.rollacoasta-keys/fly-rollmarkets.token)
#   bash infra/miner/deploy.sh 1          # the key is read from ~/.rollacoasta-keys/l3-miner-1.key (L3_MINER_KEY_FILE overrides)
#   BOOK_L3=0x… bash infra/miner/deploy.sh 1   # after a RollaBookL3 redeploy: PREDICT_BOOK and PREDICT_BOOK_L3 follow it
#   IMAGE=ghcr.io/dubthecat/rolla-miner:<sha> bash infra/miner/deploy.sh 1   # pin an image instead of :latest
#
# The key never appears in the output: it is read from the file straight into `fly secrets set`, whose own output is
# discarded. Nothing here touches the engine app.
set -euo pipefail
N=${1:?usage: deploy.sh N (1..3)}
APP=${APP:-rollmarkets-miner-$N}
REGION=${REGION:-ams}
ORG=${FLY_ORG:-personal}
KEY=${L3_MINER_KEY_FILE:-$HOME/.rollacoasta-keys/l3-miner-$N.key}
# the ACTIVE RollaBookL3 (v3, since the versioned redeploy of 2026-10-04): the orders' EIP-712 domain and the L3Final book. The
# retired rc pair (0xa1d5…4450) was the default until 2026-10-07 — a deploy without BOOK_L3= would have re-pointed the fleet to it.
BOOK_L3=${BOOK_L3:-0xfc39ab228ec5a77e681f2fa3c9491ca07456c9bc}
IMAGE=${IMAGE:-ghcr.io/dubthecat/rolla-miner:latest}
VOLUME=minerdata
HERE=$(cd "$(dirname "$0")" && pwd)
: "${FLY_API_TOKEN:?export FLY_API_TOKEN=\$(cat ~/.rollacoasta-keys/fly-rollmarkets.token) first}"
command -v fly >/dev/null || { echo "fly not on PATH (export PATH=\$PATH:\$HOME/.fly/bin)" >&2; exit 1; }
[ -f "$KEY" ] || { echo "missing key file $KEY" >&2; exit 1; }
[[ "$BOOK_L3" =~ ^0x[0-9a-fA-F]{40}$ ]] || { echo "BOOK_L3 is not an address" >&2; exit 1; }

# 1. the app
if fly status -a "$APP" >/dev/null 2>&1; then echo "app $APP exists"; else fly apps create "$APP" --org "$ORG"; fi

# 2. the volume: 1 GB in the region, one only (a miner is one machine; its journal is its own copy of the log)
if fly volumes list -a "$APP" 2>/dev/null | grep -q "$VOLUME"; then echo "volume $VOLUME exists"
else fly volumes create "$VOLUME" -a "$APP" -r "$REGION" -s 1 -y; fi

# 3. the key, staged (applied by the deploy below; nothing is printed)
fly secrets set -a "$APP" --stage L3_MINER_KEY="$(tr -d '[:space:]' < "$KEY")" >/dev/null 2>&1 && echo "secret L3_MINER_KEY set (staged)"

# 4. deploy: the template config, this app, the image, the book address; --ha=false = exactly one machine
fly deploy --config "$HERE/fly.toml" --app "$APP" --image "$IMAGE" --ha=false \
  --env PREDICT_BOOK="$BOOK_L3" --env PREDICT_BOOK_L3="$BOOK_L3"

# 5. public addresses for /healthz (a shared IPv4 and a dedicated IPv6, both free). The first deploy tries to allocate
# them itself and, under an org token, fails the IPv6 half ("org_slug is only supported with private_v6 type") — so
# they are made here, idempotently, and the app is reachable at https://$APP.fly.dev either way.
ips=$(fly ips list -a "$APP" 2>/dev/null || true)
echo "$ips" | grep -q "v4" || fly ips allocate-v4 --shared -a "$APP" >/dev/null
echo "$ips" | grep -q "v6" || fly ips allocate-v6 -a "$APP" >/dev/null
echo "deployed $APP · https://$APP.fly.dev/healthz"
