#!/bin/bash
# infra/miner/status.sh [N…] — one line per miner from its public /healthz (default: the three). `--fly` adds `fly status`.
#   bash infra/miner/status.sh          # rollmarkets-miner-1 ok shards=4 [9784:-1/-1 13839:-1/-1 …] polls=12 errors=0 up=3600s
#   bash infra/miner/status.sh --fly 2  # plus the machine table of rollmarkets-miner-2
FLY=0; NS=()
for a in "$@"; do case "$a" in --fly) FLY=1;; *) NS+=("$a");; esac; done
[ ${#NS[@]} -eq 0 ] && NS=(1 2 3)
rc=0
for N in "${NS[@]}"; do
  APP=${APP_PREFIX:-rollmarkets-miner}-$N
  body=$(curl -s --max-time 10 "https://$APP.fly.dev/healthz" || true)
  if [ -z "$body" ]; then echo "$APP UNREACHABLE"; rc=1; continue; fi
  node -e '
    const [app, body] = process.argv.slice(1); let j; try { j = JSON.parse(body); } catch { console.log(app, "BAD ANSWER", body.slice(0, 120)); process.exit(1); }
    const sh = Object.values(j.shards || {}).map((s) => `${s.shard}:${s.index}/${s.finalIndex ?? "-"}${s.dissents ? " D" + s.dissents : ""}${s.stalled ? " STALLED" : ""}${s.voteBacklog ? " B" + s.voteBacklog : ""}`);
    console.log(app, j.ok ? "ok" : "NOT OK", `miner=${j.miner}`, `shards=${j.count}`, `[${sh.join(" ")}]`, j.engine ? `listed=${j.engine.listed.length} polls=${j.engine.polls} errors=${j.engine.pollErrors}${j.engine.lastPollError ? " last=" + JSON.stringify(j.engine.lastPollError) : ""}` : "no engine", `up=${Math.round(j.uptime)}s`);
    process.exit(j.ok ? 0 : 1);' "$APP" "$body" || rc=1
  [ $FLY = 1 ] && fly status -a "$APP" 2>/dev/null | sed -n '/Machines/,$p'
done
exit $rc
